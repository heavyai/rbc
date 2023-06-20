from numba.core import ir
from numba.core.compiler import CompilerBase, DefaultPassBuilder
from numba.core.compiler_machinery import FunctionPass, register_pass
from numba.core.untyped_passes import IRProcessing

from rbc.errors import NumbaTypeError


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


class HeavyDBCompilerPipeline(CompilerBase):
    def define_pipelines(self):
        # define a new set of pipelines (just one in this case) and for ease
        # base it on an existing pipeline from the DefaultPassBuilder,
        # namely the "nopython" pipeline
        pm = DefaultPassBuilder.define_nopython_pipeline(self.state)
        # Add the new pass to run after IRProcessing
        pm.add_pass_after(CheckRaiseStmts, IRProcessing)
        # finalize
        pm.finalize()
        # return as an iterable, any number of pipelines may be defined!
        return [pm]
