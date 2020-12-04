'''Omnisci Bytes type that corresponds to Omnisci type TEXT ENCODED NONE.

Omnisci Bytes represents the following structure:

  struct Bytes {
    char* ptr;
    size_t sz;  // when non-negative, Bytes has fixed width.
  }
'''

__all__ = ['BytesPointer', 'Bytes', 'bytes_type_converter', 'OmnisciBytesType']

from rbc import typesystem
from rbc.utils import get_version
from .omnisci_buffer import (
    BufferPointer, Buffer, BufferPointerModel,
    buffer_type_converter, OmnisciBufferType,
    omnisci_buffer_constructor
)

if get_version('numba') >= (0, 49):
    from numba.core import datamodel, types, extending
else:
    from numba import datamodel, types, extending


class OmnisciBytesType(OmnisciBufferType):
    """Omnisci Bytes type for RBC typesystem.
    """


class BytesPointer(BufferPointer):
    """Type class for pointers to :code:`Omnisci Bytes` structure."""


class Bytes(Buffer):
    pass


@datamodel.register_default(BytesPointer)
class BytesPointerModel(BufferPointerModel):
    pass


def bytes_type_converter(obj):
    """Return Type instance corresponding to Omnisci :code:`Bytes` type.

    :code:`Omnisci Bytes` is defined as follows (using C++ syntax)::

      struct Column {
        char* ptr;
        size_t sz;
        bool is_null;
      }

    See :code:`buffer_type_converter` for details.
    """
    return buffer_type_converter(
        obj, OmnisciBytesType, 'Bytes', BytesPointer,
        extra_members=[typesystem.Type.fromstring('bool is_null')],
        element_type='char').pointer()


@extending.lower_builtin(Bytes, types.Integer)
def omnisci_bytes_constructor(context, builder, sig, args):
    return omnisci_buffer_constructor(context, builder, sig, args)


@extending.type_callable(Bytes)
def type_omnisci_bytes(context):
    def typer(size):
        atyp = bytes_type_converter('Bytes')
        return atyp.tonumba()
    return typer
