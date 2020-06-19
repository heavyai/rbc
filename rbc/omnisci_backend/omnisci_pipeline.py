from rbc.utils import get_version
from llvmlite import ir as llvm_ir
from .omnisci_array import builder_buffers, ArrayPointer
if get_version('numba') >= (0, 49):
    from numba.core import ir, extending, types
    from numba.core.compiler import CompilerBase, DefaultPassBuilder
    from numba.core.compiler_machinery import FunctionPass, register_pass
    from numba.core.untyped_passes import IRProcessing
else:
    from numba import ir, extending, types
    from numba.compiler import CompilerBase, DefaultPassBuilder
    from numba.compiler_machinery import FunctionPass, register_pass
    from numba.untyped_passes import IRProcessing

int8_t = llvm_ir.IntType(8)
int32_t = llvm_ir.IntType(32)
int64_t = llvm_ir.IntType(64)
void_t = llvm_ir.VoidType()


@extending.intrinsic
def free_omnisci_array(typingctx, ret):
    sig = types.void(ret)

    def codegen(context, builder, signature, args):
        buffers = builder_buffers[builder]

        # TODO: using stdlib `free` that works only for CPU. For CUDA
        # devices, we need to use omniscidb provided deallocator.
        free_fnty = llvm_ir.FunctionType(void_t, [int8_t.as_pointer()])
        free_fn = builder.module.get_or_insert_function(free_fnty, name="free")

        # We skip the ret pointer iff we're returning an Array
        # otherwise, we free everything
        if isinstance(ret, ArrayPointer):
            [arg] = args
            ptr = builder.load(builder.gep(arg, [int32_t(0), int32_t(0)]))
            ptr = builder.bitcast(ptr, int8_t.as_pointer())
            for ptr8 in buffers:
                with builder.if_then(builder.icmp_signed('!=', ptr, ptr8)):
                    builder.call(free_fn, [ptr8])
        else:
            for ptr8 in buffers:
                builder.call(free_fn, [ptr8])

        del builder_buffers[builder]

    return sig, codegen


# Register this pass with the compiler framework, declare that it will not
# mutate the control flow graph and that it is not an analysis_only pass (it
# potentially mutates the IR).
@register_pass(mutates_CFG=False, analysis_only=False)
class FreeOmnisciArray(FunctionPass):
    _name = "free_omnisci_arrays"  # the common name for the pass

    def __init__(self):
        FunctionPass.__init__(self)

    # implement method to do the work, "state" is the internal compiler
    # state from the CompilerBase instance.
    def run_pass(self, state):
        func_ir = state.func_ir  # get the FunctionIR object

        for blk in func_ir.blocks.values():
            for stmt in blk.find_insts(ir.Assign):
                if isinstance(stmt.value, ir.FreeVar) \
                   and stmt.value.name == 'Array':
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

                name = 'free_omnisci_array_fn'
                value = ir.Global(name, free_omnisci_array, loc)
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
        pm.add_pass_after(FreeOmnisciArray, IRProcessing)
        # finalize
        pm.finalize()
        # return as an iterable, any number of pipelines may be defined!
        return [pm]
