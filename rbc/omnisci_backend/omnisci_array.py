"""Implement Omnisci Array type support
"""

__all__ = ['ArrayPointer', 'Array', 'OmnisciArrayType']

from rbc import typesystem, errors
from .omnisci_buffer import (BufferPointer, Buffer,
                             OmnisciBufferType,
                             omnisci_buffer_constructor)
from numba.core import extending, types
from typing import Union, TypeVar


T = TypeVar('T')


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
    """
    In HeavyDB, an Array of type `T` is represented as follows:

    .. code-block:: C

        {
            T* data,
            int64_t size,
            int8_t is_null
        }

    Array is a contiguous block of memory that behaves like a python list.

    Example


    .. code-block:: python

        from numba import types
        from rbc.omnisci_backend import Array

        @omnisci('int64[](int64)')
        def create_and_fill_array(size):
            arr = Array(size, types.int64)
            for i in range(size):
                a[i] = i
            return a
    """

    def __init__(self, size: int, dtype: Union[str, types.Type]) -> None:
        pass

    def is_null(self) -> bool:
        pass

    @property
    def dtype(self):
        """
        Data type of the array elements.
        """
        pass

    @property
    def device(self):
        """
        ❌ Not implemented

        Hardware device the array data resides on.
        """
        pass

    @property
    def mT(self):
        """
        ❌ Not implemented

        Transpose of a amtrix (or a stack of matrices).
        """
        pass

    @property
    def ndim(self):
        """
        ❌ Not implemented

        Number of array dimensions (axes).
        """
        pass

    @property
    def shape(self):
        """
        ❌ Not implemented

        Array dimensions.
        """
        pass

    @property
    def size(self):
        """
        ❌ Not implemented

        Number of elements in an array.
        """
        pass

    @property
    def T(self):
        """
        ❌ Not implemented

        Transpose of the array.
        """
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
