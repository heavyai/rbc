import operator
import warnings

from rbc.errors import NumbaTypeError
from .omnisci_buffer import BufferPointer, free_buffer
from numba.core import ir, types
from numba.core.compiler import CompilerBase, DefaultPassBuilder
from numba.core.compiler_machinery import FunctionPass, register_pass
from numba.core.untyped_passes import (IRProcessing,
                                       RewriteSemanticConstants,
                                       ReconstructSSA,
                                       DeadBranchPrune,)
from numba.core.typed_passes import (PartialTypeInference,
                                     DeadCodeElimination,
                                     NopythonTypeInference)


@register_pass(mutates_CFG=False, analysis_only=False)
class CheckRaiseStmts(FunctionPass):
    _name = "check_raise_stmts"

    def __init__(self):
        FunctionPass.__init__(self)

    def run_pass(self, state):
        func_ir = state.func_ir
        for blk in func_ir.blocks.values():
            for _raise in blk.find_insts(ir.Raise):
                msg = ('raise statement is not supported in '
                       'UDF/UDTFs. Please, use `return table_function_error(msg)` '
                       'to raise an error.')
                loc = _raise.loc
                raise NumbaTypeError(msg, loc=loc)
        return False


@register_pass(mutates_CFG=False, analysis_only=False)
class DTypeComparison(FunctionPass):
    _name = "DTypeComparison"

    def __init__(self):
        FunctionPass.__init__(self)

    def is_dtype_comparison(self, func_ir, binop):
        """ Return True if binop is a dtype comparison
        """
        def is_getattr(expr):
            return isinstance(expr, ir.Expr) and expr.op == 'getattr'

        if binop.fn != operator.eq:
            return False

        lhs = func_ir.get_definition(binop.lhs.name)
        rhs = func_ir.get_definition(binop.lhs.name)

        return (is_getattr(lhs) and lhs.attr == 'dtype') or \
               (is_getattr(rhs) and rhs.attr == 'dtype')

    def run_pass(self, state):
        # run as subpipeline
        from numba.core.compiler_machinery import PassManager
        pm = PassManager("subpipeline")
        pm.add_pass(PartialTypeInference, "performs partial type inference")
        pm.finalize()
        pm.run(state)

        mutated = False

        func_ir = state.func_ir
        for block in func_ir.blocks.values():
            for assign in block.find_insts(ir.Assign):
                binop = assign.value
                if not (isinstance(binop, ir.Expr) and binop.op == 'binop'):
                    continue
                if self.is_dtype_comparison(func_ir, binop):
                    var = func_ir.get_assignee(binop)
                    typ = state.typemap.get(var.name, None)
                    if isinstance(typ, types.BooleanLiteral):
                        loc = binop.loc
                        rhs = ir.Const(typ.literal_value, loc)
                        new_assign = ir.Assign(rhs, var, loc)

                        # replace instruction
                        block.insert_after(new_assign, assign)
                        block.remove(assign)
                        mutated = True

        if mutated:
            pm = PassManager("subpipeline")
            # rewrite consts / dead branch pruning
            pm.add_pass(DeadCodeElimination, "dead code elimination")
            pm.add_pass(RewriteSemanticConstants, "rewrite semantic constants")
            pm.add_pass(DeadBranchPrune, "dead branch pruning")
            pm.finalize()
            pm.run(state)
        return mutated


class MissingFreeWarning(Warning):

    _msg = """
    Possible memory leak detected!

    Arrays or buffers are allocated by function {func_name} but the function
    never calls .free() or free_buffer().
    In RBC, arrays and buffers must be freed manually: the relevant docs.
    """

    def __init__(self, func_name):
        msg = self.make_message(func_name)
        Warning.__init__(self, msg)

    @classmethod
    def make_message(cls, func_name):
        return cls._msg.format(func_name=func_name)


class MissingFreeError(Exception):

    def __init__(self, func_name):
        msg = MissingFreeWarning.make_message(func_name)
        Exception.__init__(self, msg)


@register_pass(mutates_CFG=False, analysis_only=True)
class DetectMissingFree(FunctionPass):
    _name = "DetectMissingFree"

    def __init__(self):
        FunctionPass.__init__(self)

    def iter_calls(self, func_ir):
        for block in func_ir.blocks.values():
            for inst in block.find_insts(ir.Assign):
                if isinstance(inst.value, ir.Expr) and inst.value.op == 'call':
                    yield inst

    def contains_buffer_constructors(self, state):
        """
        Check whether the func_ir contains any call which creates a buffer. This
        could be either a direct call to e.g. xp.Array() or a call to any
        function which returns a BufferPointer: in that case we assume that
        the ownership is transfered upon return and that the caller is
        responsible to free() it.
        """
        func_ir = state.func_ir
        for inst in self.iter_calls(func_ir):
            ret_type = state.typemap[inst.target.name]
            if isinstance(ret_type, BufferPointer):
                return True
        return False

    def is_free_buffer(self, rhs):
        return isinstance(rhs, (ir.Global, ir.FreeVar)) and rhs.value is free_buffer

    def is_BufferPoint_dot_free(self, state, expr):
        return (isinstance(expr, ir.Expr) and
                expr.op == 'getattr' and
                isinstance(state.typemap[expr.value.name], BufferPointer) and
                expr.attr == 'free')

    def contains_calls_to_free(self, state):
        """
        Check whether there is at least an instruction which calls free_buffer()
        or BufferPointer.free()
        """
        func_ir = state.func_ir
        for inst in self.iter_calls(func_ir):
            rhs = func_ir.get_definition(inst.value.func)
            if self.is_free_buffer(rhs) or self.is_BufferPoint_dot_free(state, rhs):
                return True
        return False

    def run_pass(self, state):
        on_missing_free = state.flags.on_missing_free
        if (self.contains_buffer_constructors(state) and not self.contains_calls_to_free(state)):
            func_name = state.func_id.func.__name__
            if on_missing_free == 'warn':
                warnings.warn(MissingFreeWarning(func_name))
            elif on_missing_free == 'fail':
                raise MissingFreeError(func_name)
            else:
                raise ValueError(
                    f"Unexpected value for on_missing_free: "
                    f"got {on_missing_free:r}, expected 'warn', 'fail' or 'ignore'"
                )
        return False  # we didn't modify the IR


class OmnisciCompilerPipeline(CompilerBase):
    def define_pipelines(self):
        # define a new set of pipelines (just one in this case) and for ease
        # base it on an existing pipeline from the DefaultPassBuilder,
        # namely the "nopython" pipeline
        pm = DefaultPassBuilder.define_nopython_pipeline(self.state)
        # Add the new pass to run after IRProcessing
        pm.add_pass_after(CheckRaiseStmts, IRProcessing)
        pm.add_pass_after(DTypeComparison, ReconstructSSA)
        if self.state.flags.on_missing_free != 'ignore':
            pm.add_pass_after(DetectMissingFree, NopythonTypeInference)
        # finalize
        pm.finalize()
        # return as an iterable, any number of pipelines may be defined!
        return [pm]
