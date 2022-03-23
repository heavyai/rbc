import operator

from rbc.errors import NumbaTypeError
from numba.core import ir, types
from numba.core.compiler import CompilerBase, DefaultPassBuilder
from numba.core.compiler_machinery import FunctionPass, register_pass
from numba.core.untyped_passes import (IRProcessing,
                                       RewriteSemanticConstants,
                                       ReconstructSSA,
                                       DeadBranchPrune,)
from numba.core.typed_passes import PartialTypeInference, DeadCodeElimination


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


class HeavyDBCompilerPipeline(CompilerBase):
    def define_pipelines(self):
        # define a new set of pipelines (just one in this case) and for ease
        # base it on an existing pipeline from the DefaultPassBuilder,
        # namely the "nopython" pipeline
        pm = DefaultPassBuilder.define_nopython_pipeline(self.state)
        # Add the new pass to run after IRProcessing
        pm.add_pass_after(CheckRaiseStmts, IRProcessing)
        pm.add_pass_after(DTypeComparison, ReconstructSSA)
        # finalize
        pm.finalize()
        # return as an iterable, any number of pipelines may be defined!
        return [pm]
