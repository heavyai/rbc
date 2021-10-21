"""External functions defined by the OmniSciDB server
"""


import functools
from rbc import irutils
from rbc.errors import UnsupportedError
from rbc.targetinfo import TargetInfo
from numba.core import extending, types as nb_types
from numba.core.cgutils import make_bytearray, global_constant
from llvmlite import ir


@extending.intrinsic
def set_output_row_size(typingctx, set_output_row_size):
    """``set_output_row_size`` sets the row size of output Columns and
    allocates the corresponding column buffers

    .. note::
        ``set_output_row_size`` is available only for CPU target and OmniSciDB v5.7 or newer
    """
    # void is declared as 'none' in Numba and 'none' is converted to a void* (int8*). See:
    # https://github.com/numba/numba/blob/6881dfe3883d1344014ea16185ed87de4b75b9a1/numba/core/types/__init__.py#L95
    sig = nb_types.void(nb_types.int64)

    def codegen(context, builder, sig, args):
        target_info = TargetInfo()
        if target_info.software[1][:3] < (5, 7, 0):
            msg = 'set_output_row_size is only available in OmniSciDB 5.7 or newer'
            raise UnsupportedError(msg)

        fnty = ir.FunctionType(ir.VoidType(), [ir.IntType(64)])
        fn = irutils.get_or_insert_function(builder.module, fnty, name="set_output_row_size")
        assert fn.is_declaration
        builder.call(fn, args)  # don't return anything

    return sig, codegen


@extending.intrinsic
def table_function_error(typingctx, message):
    #cchar = nb_types.RawPointer('const char*')
    if message != nb_types.int64:
        # XXX: message should be a cchar, not an int64
        return None

    def codegen(context, builder, sig, args):
        int8ptr = ir.PointerType(ir.IntType(8))
        fntype = ir.FunctionType(ir.IntType(32), [int8ptr])
        fn = irutils.get_or_insert_function(builder.module, fntype,
                                            name="table_function_error")
        assert fn.is_declaration
        #
        # WIP: we should pass message, not 'hello world'
        msg_const = make_bytearray(b'hello world\0')
        msg_global_var = global_constant(builder.module, "my_message", msg_const)
        msg_ptr = builder.bitcast(msg_global_var, int8ptr)
        return builder.call(fn, [msg_ptr])

    sig = nb_types.int32(nb_types.int64) # XXX, use cchar
    return sig, codegen


# fix docstring for intrinsics
for __func in (set_output_row_size,):
    functools.update_wrapper(__func, __func._defn)
