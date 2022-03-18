'''Omnisci Bytes type that corresponds to Omnisci type TEXT ENCODED NONE.

Omnisci Bytes represents the following structure:

  struct Bytes {
    char* ptr;
    size_t sz;  // when non-negative, Bytes has fixed width.
  }
'''

__all__ = ['BytesPointer', 'Bytes', 'OmnisciBytesType']

import operator
from llvmlite import ir
from rbc import typesystem
from .buffer import (
    BufferPointer, Buffer, OmnisciBufferType,
    omnisci_buffer_constructor)
from numba.core import types, extending, cgutils


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)


class OmnisciBytesType(OmnisciBufferType):
    """Omnisci Bytes type for RBC typesystem.
    """

    @property
    def buffer_extra_members(self):
        return ('bool is_null',)

    def match(self, other):
        if type(self) is type(other):
            return self[0] == other[0]
        if other.is_pointer and other[0].is_char and other[0].bits == 8:
            return 1
        if other.is_string:
            return 2


class BytesPointer(BufferPointer):
    pass


class Bytes(Buffer):
    pass


@extending.intrinsic
def omnisci_bytes_ptr_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index, value = args

        rawptr = cgutils.alloca_once_value(builder, value=data)
        ptr = builder.load(rawptr)

        buf = builder.load(builder.gep(ptr, [zero, zero]))
        trunc = builder.trunc(value, buf.type.pointee)
        builder.store(trunc, builder.gep(buf, [index]))

    return sig, codegen


@extending.lower_builtin(Bytes, types.Integer)
def omnisci_bytes_constructor(context, builder, sig, args):
    return omnisci_buffer_constructor(context, builder, sig, args)


@extending.type_callable(Bytes)
def type_omnisci_bytes(context):
    def typer(size):
        return typesystem.Type.fromobject('Bytes').tonumba()
    return typer


@extending.overload(operator.setitem)
def omnisci_bytes_setitem(a, i, v):
    if isinstance(a, BytesPointer):
        return lambda a, i, v: omnisci_bytes_ptr_setitem_(a, i, v)
