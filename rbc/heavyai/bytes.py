'''Omnisci Bytes type that corresponds to Omnisci type TEXT ENCODED NONE.

Omnisci Bytes represents the following structure:

  struct Bytes {
    char* ptr;
    size_t sz;  // when non-negative, Bytes has fixed width.
  }
'''

__all__ = ['BytesPointer', 'Bytes', 'OmnisciBytesType']

from rbc import typesystem
from .buffer import (
    BufferPointer, Buffer, OmnisciBufferType,
    omnisci_buffer_constructor)
from numba.core import types, extending


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


BytesPointer = BufferPointer


class Bytes(Buffer):
    pass


@extending.lower_builtin(Bytes, types.Integer)
def omnisci_bytes_constructor(context, builder, sig, args):
    return omnisci_buffer_constructor(context, builder, sig, args)


@extending.type_callable(Bytes)
def type_omnisci_bytes(context):
    def typer(size):
        return typesystem.Type.fromobject('Bytes').tonumba()
    return typer
