"""Implement Omnisci Array type support
"""

__all__ = ['ArrayPointer', 'Array', 'OmnisciArrayType']

from rbc import typesystem, errors
from .omnisci_buffer import (BufferPointer, Buffer,
                             OmnisciBufferType,
                             omnisci_buffer_constructor)
from numba.core import extending, types


class OmnisciArrayType(OmnisciBufferType):
    """Omnisci Array type for RBC typesystem.

    :code:`Omnisci Array<T>` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Array {
        T* ptr;
        size_t sz;
        bool is_null;
      }
    """
    @property
    def buffer_extra_members(self):
        return ('bool is_null',)


ArrayPointer = BufferPointer


class Array(Buffer):
    pass


@extending.lower_builtin(Array, types.Integer, types.StringLiteral)
@extending.lower_builtin(Array, types.Integer, types.NumberClass)
def omnisci_array_constructor(context, builder, sig, args):
    return omnisci_buffer_constructor(context, builder, sig, args)


@extending.type_callable(Array)
def type_omnisci_array(context):
    def typer(size, dtype):
        if isinstance(dtype, types.StringLiteral):
            element_type = typesystem.Type.fromstring(dtype.literal_value)
        elif isinstance(dtype, types.NumberClass):
            element_type = typesystem.Type.fromobject(dtype)
        else:
            raise errors.NumbaNotImplementedError(repr(dtype))
        return OmnisciArrayType((element_type,)).tonumba()
    return typer
