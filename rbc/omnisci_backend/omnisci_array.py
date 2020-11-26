"""Implement Omnisci Array type support
"""

__all__ = ['ArrayPointer', 'Array', 'omnisci_array_constructor',
           'array_type_converter', 'OmnisciArrayType']

from rbc import typesystem
from rbc.utils import get_version
from .omnisci_buffer import (BufferPointer, Buffer, BufferPointerModel,
                             buffer_type_converter, OmnisciBufferType,
                             omnisci_buffer_constructor)

if get_version('numba') >= (0, 49):
    from numba.core import datamodel, extending, types
else:
    from numba import datamodel, extending, types


class OmnisciArrayType(OmnisciBufferType):
    """Omnisci Array type for RBC typesystem.
    """


class ArrayPointer(BufferPointer):
    """Type class for pointers to :code:`Omnisci Array<T>` structure."""


class Array(Buffer):
    pass


@datamodel.register_default(ArrayPointer)
class ArrayPointerModel(BufferPointerModel):
    pass


@extending.lower_builtin(Array, types.Integer, types.StringLiteral)
@extending.lower_builtin(Array, types.Integer, types.NumberClass)
def omnisci_array_constructor(context, builder, sig, args):
    return omnisci_buffer_constructor(context, builder, sig, args)


@extending.type_callable(Array)
def type_omnisci_array(context):
    def typer(size, dtype):
        if isinstance(dtype, types.StringLiteral):
            typ = 'Array<{}>'.format(dtype.literal_value)
        elif isinstance(dtype, types.NumberClass):
            typ = 'Array<{}>'.format(dtype.dtype)
        else:
            raise NotImplementedError(repr(dtype))

        atyp = array_type_converter(typ)
        if atyp is not None:
            return atyp.tonumba()
        raise NotImplementedError((dtype, typ))
    return typer


def array_type_converter(obj):
    """Return Type instance corresponding to :code:`Omnisci Array<T>` type.

    :code:`Omnisci Array<T>` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Array {
        T* ptr;
        size_t sz;
        bool is_null;
      }

    See :code:`buffer_type_converter` for details.
    """
    buffer_type = buffer_type_converter(
        obj, OmnisciArrayType, 'Array', ArrayPointer,
        extra_members=[typesystem.Type.fromstring('bool is_null')])
    if buffer_type is not None:
        return buffer_type.pointer()
    return buffer_type
