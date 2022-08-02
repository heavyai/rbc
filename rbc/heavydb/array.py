"""Implement HeavyDB Array type support
"""

__all__ = ['ArrayPointer', 'Array', 'HeavyDBArrayType']

from rbc import typesystem, errors
from .buffer import (BufferPointer, Buffer,
                     HeavyDBBufferType,
                     heavydb_buffer_constructor)
from numba.core import extending, cgutils
from numba import types as nb_types
from typing import Union
from llvmlite import ir


int32_t = ir.IntType(32)
int64_t = ir.IntType(64)


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
    def numba_pointer_type(self):
        return ArrayPointer

    @property
    def buffer_extra_members(self):
        return ('bool is_null',)


class ArrayPointer(BufferPointer):
    def deepcopy(self, context, builder, val, retptr):
        from .buffer import memalloc
        ptr_type = self.dtype.members[0]
        element_size = int64_t(ptr_type.dtype.bitwidth // 8)

        struct_load = builder.load(val, name='struct_load')
        src = builder.extract_value(struct_load, 0, name='array_buff_ptr')
        element_count = builder.extract_value(struct_load, 1, name='array_size')
        is_null = builder.extract_value(struct_load, 2, name='array_is_null')

        zero, one, two = int32_t(0), int32_t(1), int32_t(2)
        with builder.if_else(cgutils.is_true(builder, is_null)) as (then, otherwise):
            with then:
                nullptr = cgutils.get_null_value(src.type)
                builder.store(nullptr, builder.gep(retptr, [zero, zero]))
            with otherwise:
                # we can't just copy the pointer here because return buffers need
                # to have their own memory, as input buffers are freed upon returning
                ptr = memalloc(context, builder, ptr_type, element_count, element_size)
                cgutils.raw_memcpy(builder, ptr, src, element_count, element_size)
                builder.store(ptr, builder.gep(retptr, [zero, zero]))
        builder.store(element_count, builder.gep(retptr, [zero, one]))
        builder.store(is_null, builder.gep(retptr, [zero, two]))


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
