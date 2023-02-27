__all__ = ['allocate_varlen_buffer']

from numba.core import cgutils
from llvmlite import ir


def allocate_varlen_buffer(builder, size):
    i8p = ir.IntType(8).as_pointer()
    i64 = ir.IntType(64)

    module = builder.module
    name = 'allocate_varlen_buffer'
    fnty = ir.FunctionType(i8p, [i64, i64])
    fn = cgutils.get_or_insert_function(module, fnty, name)
    return builder.call(fn, [size, i64(1)])
