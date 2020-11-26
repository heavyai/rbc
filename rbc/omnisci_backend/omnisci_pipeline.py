from rbc.utils import get_version
from .omnisci_buffer import BufferMeta, free_omnisci_buffer
if get_version('numba') >= (0, 49):
    from numba.core import ir
    from numba.core.compiler import CompilerBase, DefaultPassBuilder
    from numba.core.compiler_machinery import FunctionPass, register_pass
    from numba.core.untyped_passes import IRProcessing
else:
    from numba import ir
    from numba.compiler import CompilerBase, DefaultPassBuilder
    from numba.compiler_machinery import FunctionPass, register_pass
    from numba.untyped_passes import IRProcessing


# Register this pass with the compiler framework, declare that it will not
# mutate the control flow graph and that it is not an analysis_only pass (it
# potentially mutates the IR).
@register_pass(mutates_CFG=False, analysis_only=False)
class FreeOmnisciBuffer(FunctionPass):
    _name = "free_omnisci_buffers"  # the common name for the pass

    def __init__(self):
        FunctionPass.__init__(self)

    # implement method to do the work, "state" is the internal compiler
    # state from the CompilerBase instance.
    def run_pass(self, state):
        func_ir = state.func_ir  # get the FunctionIR object

        for blk in func_ir.blocks.values():
            for stmt in blk.find_insts(ir.Assign):
                if isinstance(stmt.value, ir.FreeVar) \
                   and stmt.value.name in BufferMeta.class_names:
                    break
            else:
                continue
            break
        else:
            return False  # one does not changes the IR

        for blk in func_ir.blocks.values():
            loc = blk.loc
            scope = blk.scope
            for ret in blk.find_insts(ir.Return):

                name = 'free_omnisci_buffer_fn'
                value = ir.Global(name, free_omnisci_buffer, loc)
                target = scope.make_temp(loc)
                stmt = ir.Assign(value, target, loc)
                blk.insert_before_terminator(stmt)

                fn_call = ir.Expr.call(func=target, args=[ret.value],
                                       kws=(), loc=loc)
                lhs = scope.make_temp(loc)
                var = ir.Assign(fn_call, lhs, blk.loc)
                blk.insert_before_terminator(var)
                break

        return True  # we changed the IR


class OmnisciCompilerPipeline(CompilerBase):

    def define_pipelines(self):
        # define a new set of pipelines (just one in this case) and for ease
        # base it on an existing pipeline from the DefaultPassBuilder,
        # namely the "nopython" pipeline
        pm = DefaultPassBuilder.define_nopython_pipeline(self.state)
        # Add the new pass to run after IRProcessing
        pm.add_pass_after(FreeOmnisciBuffer, IRProcessing)
        # finalize
        pm.finalize()
        # return as an iterable, any number of pipelines may be defined!
        return [pm]
