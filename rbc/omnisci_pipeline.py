from numba.core import ir, extending, ir_utils, cgutils
from numba.core.compiler import CompilerBase, DefaultPassBuilder
from numba.core.compiler_machinery import FunctionPass, register_pass
from numba.core.untyped_passes import IRProcessing
from numbers import Number
from numba import njit, types

@extending.intrinsic
def teste(typingctx):
    sig = types.void()

    def codegen(context, builder, signature, args):
        cgutils.printf(builder, "Teste!!\n")

    return sig, codegen


# Register this pass with the compiler framework, declare that it will not
# mutate the control flow graph and that it is not an analysis_only pass (it
# potentially mutates the IR).
@register_pass(mutates_CFG=True, analysis_only=False)
class FreeOmnisciArray(FunctionPass):
    _name = "free_omnisci_arrays" # the common name for the pass

    def __init__(self):
        FunctionPass.__init__(self)

    # implement method to do the work, "state" is the internal compiler
    # state from the CompilerBase instance.
    def run_pass(self, state):
        func_ir = state.func_ir # get the FunctionIR object
        mutated = True # used to record whether this pass mutates the IR
        for blk in func_ir.blocks.values():
            loc = blk.loc
            scope = blk.scope
            for inst in blk.find_insts(ir.Return):

                value = ir.Global("teste", teste, loc)
                target = ir.Var(scope, "load_teste", loc)
                stmt = ir.Assign(value, target, loc)
                blk.insert_before_terminator(stmt)

                new_expr = ir.Expr.call(func=target, args=[],
                                        kws=(), loc=loc)
                lhs = ir.Var(blk.scope, "teste_var_lhs", blk.loc)
                var = ir.Assign(new_expr, lhs, blk.loc)
                blk.insert_before_terminator(var)
                break

        return mutated # return True if the IR was mutated, False if not.


class OmnisciCompilerPipeline(CompilerBase): # custom compiler extends from CompilerBase

    def define_pipelines(self):
        # define a new set of pipelines (just one in this case) and for ease
        # base it on an existing pipeline from the DefaultPassBuilder,
        # namely the "nopython" pipeline
        pm = DefaultPassBuilder.define_nopython_pipeline(self.state)
        # Add the new pass to run after IRProcessing
        pm.add_pass_after(FreeOmnisciArray, IRProcessing)
        # finalize
        pm.finalize()
        # return as an iterable, any number of pipelines may be defined!
        return [pm]
