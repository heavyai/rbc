'''Omnisci Bytes type that corresponds to Omnisci type TEXT ENCODED NONE.

Omnisci Bytes represents the following structure:

  struct Bytes {
    char* ptr;
    size_t sz;  // when non-negative, Bytes has fixed width.
  }
'''

__all__ = ['BytesPointer', 'Bytes', 'OmnisciBytesType']

from rbc import typesystem
from .omnisci_buffer import (
    BufferPointer, Buffer, OmnisciBufferType,
    omnisci_buffer_constructor)
from numba.core import types, extending


class OmnisciBytesType(OmnisciBufferType):
    """Omnisci Bytes type for RBC typesystem.
    """

    @property
    def buffer_extra_members(self):
        return ('bool is_null',)


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
