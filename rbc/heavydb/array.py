"""Implement HeavyDB Array type support
"""

__all__ = ['ArrayPointer', 'Array', 'HeavyDBArrayType']

from rbc import typesystem, errors
from .buffer import (BufferPointer, Buffer,
                     HeavyDBBufferType,
                     heavydb_buffer_constructor)
from numba.core import extending
from numba import types as nb_types
from typing import Union


class HeavyDBArrayType(HeavyDBBufferType):
    """HeavyDB Array type for RBC typesystem.

    :code:`HeavyDB Array<T>` is defined as follows (using C++ syntax)::

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
    RBC ``Array<T>`` type that corresponds to HeavyDB ARRAY


    In HeavyDB, an Array of type ``T`` is represented as follows:

    .. code-block:: C

        {
            T* data,
            int64_t size,
            int8_t is_null
        }

    Array holds a contiguous block of memory and it implements a
    subset of the array protocol.

    Example

    .. code-block:: python

        from numba import types
        from rbc.heavydb import Array

        @heavydb('int64[](int64)')
        def my_arange(size):
            arr = Array(size, nb_types.int64)
            for i in range(size):
                a[i] = i
            return a
    """

    def __init__(self, size: int, dtype: Union[str, nb_types.Type]) -> None:
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

        Transpose of a matrix (or a stack of matrices).
        """
        pass

    @property
    def ndim(self):
        """
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


@extending.lower_builtin(Array, nb_types.Integer, nb_types.StringLiteral)
@extending.lower_builtin(Array, nb_types.Integer, nb_types.NumberClass)
def heavydb_array_constructor(context, builder, sig, args):
    return heavydb_buffer_constructor(context, builder, sig, args)._getpointer()


@extending.type_callable(Array)
def type_heavydb_array(context):
    def typer(size, dtype):
        if isinstance(dtype, nb_types.StringLiteral):
            element_type = typesystem.Type.fromstring(dtype.literal_value)
        elif isinstance(dtype, nb_types.NumberClass):
            element_type = typesystem.Type.fromobject(dtype)
        else:
            raise errors.NumbaNotImplementedError(repr(dtype))
        return HeavyDBArrayType((element_type,)).tonumba()
    return typer


@extending.overload_attribute(ArrayPointer, 'ndim')
def get_ndim(arr):
    def impl(arr):
        return 1
    return impl


@extending.overload_attribute(ArrayPointer, 'size')
def get_size(arr):
    def impl(arr):
        return len(arr)
    return impl
