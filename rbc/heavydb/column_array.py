
"""Implement Heavydb Column<Array<T>> type support

Heavydb Column<Array<T>> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['HeavyDBOutputColumnArrayType', 'HeavyDBColumnArrayType', 'ColumnArray']

import operator
from typing import TypeVar
from rbc import typesystem
from .column import HeavyDBColumnType
from . import array
from .metatype import HeavyDBMetaType
from numba.core import extending, cgutils
from numba.core import types as nb_types
from llvmlite import ir


T = TypeVar('T')


i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()


_COLUMN_PARAM_NAME = 'ColumnArray_inner_type'


class ColumnArray(metaclass=HeavyDBMetaType):
    """
    RBC ``Column<Array<T>>`` type that corresponds to HeavyDB COLUMN<ARRAY<T>>

    In HeavyDB, a Column of type ``Array<T>`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """
    def get_item(self, index: int) -> int:
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, arr: 'array.Array[T]') -> int:
        """
        ``self[index] = arr``

        .. note::
            Only available on ``CPU``
        """

    def concat_item(self, index: int, arr: 'array.Array[T]') -> int:
        """
        Concat array ``arr`` at ``index``.

        .. note::
            Only available on ``CPU``
        """

    def is_null(self, index: int) -> int:
        """
        Check if the array at ``index`` is null.

        .. note::
            Only available on ``CPU``
        """

    def set_null(self, index: int) -> int:
        """
        Set the array at ``index`` to null.

        .. note::
            Only available on ``CPU``
        """

    def __len__(self) -> int:
        """
        Return the length of the array.

        ..  note::
             Only available on ``CPU``
        """


class HeavyDBColumnArrayType(HeavyDBColumnType):

    def postprocess_type(self):
        return self

    @property
    def numba_type(self):
        return ColumnArrayType

    @property
    def numba_pointer_type(self):
        return ColumnArrayPointer

    @property
    def element_type(self):
        return typesystem.Type.fromstring('int8_t')

    @property
    def custom_params(self):
        return {
            **super().custom_params,
            _COLUMN_PARAM_NAME: self[0][0],
            'name': f'STRUCT_{self.mangling()}_{self[0][0][0][0].tostring()}'
        }


class HeavyDBOutputColumnArrayType(HeavyDBColumnArrayType):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn is the same as Column<Array<T>> but introduced to distinguish
    the input and output arguments of UDTFs.
    """


class ColumnArrayType(nb_types.Type):
    """Numba type class for HeavyDB buffer structures.
    """

    def __init__(self, dtype):
        array_T = self.__typesystem_type__._params.get(_COLUMN_PARAM_NAME)
        eltype = array_T.tonumba().eltype
        self.dtype = array_T.tonumba()  # struct dtype
        name = f"Column<Array<{eltype}>>"
        super().__init__(name)

    @property
    def key(self):
        return self.dtype

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.dtype.eltype


class ColumnArrayPointer(nb_types.Type):
    """Numba type class for pointers to HeavyDB buffer structures.

    We are not deriving from CPointer because ColumnArrayPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        self.eltype = dtype.eltype  # buffer element dtype
        name = f"Column<Array<{self.eltype}>>*"
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


@extending.intrinsic
def heavydb_column_array_getitem_(typingctx, x, i):
    T = x.eltype
    array = typesystem.Type.fromstring(f'Array<{T}>').tonumba().dtype
    sig = array(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        fa = cgutils.create_struct_proxy(sig.return_type)(context, builder)
        fnty = ir.FunctionType(void, [i8p, i64, i64, i8pp, i64p, i8p, i64])
        getItem = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnArray_getItem")
        flatbuffer = builder.extract_value(col, [0])
        ptr = builder.alloca(fa.ptr.type)
        size = builder.alloca(fa.sz.type)
        is_null = builder.alloca(fa.is_null.type)
        expected_numel = i64(-1)
        element_size = i64(array.eltype.bitwidth // 8)
        builder.call(getItem, [flatbuffer, index, expected_numel,
                               builder.bitcast(ptr, i8pp), size,
                               is_null, element_size])
        fa.ptr = builder.load(ptr)
        fa.sz = builder.sdiv(builder.load(size), element_size)
        fa.is_null = builder.load(is_null)
        return fa._getvalue()

    return sig, codegen


@extending.intrinsic
def heavydb_column_array_setitem_(typingctx, x, i, arr):
    sig = nb_types.void(x, i, arr)

    def codegen(context, builder, sig, args):
        col, index, arr_ = args
        fnty = ir.FunctionType(void, [i8p, i64, i8p, i64, i8, i64])
        getItem = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnArray_setItem")
        flatbuffer = builder.extract_value(col, [0])
        element_size = i64(arr.eltype.bitwidth // 8)
        fa = cgutils.create_struct_proxy(arr)(context, builder, value=arr_)
        builder.call(getItem, [flatbuffer, index, builder.bitcast(fa.ptr, i8p),
                               fa.sz, fa.is_null, element_size])

    return sig, codegen


@extending.intrinsic
def heavydb_column_array_concatitem_(typingctx, x, i, arr):
    sig = nb_types.void(x, i, arr)

    def codegen(context, builder, sig, args):
        col, index, arr_ = args
        fnty = ir.FunctionType(void, [i8p, i64, i8p, i64, i8, i64])
        getItem = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnArray_concatItem")
        flatbuffer = builder.extract_value(col, [0])
        element_size = i64(arr.eltype.bitwidth // 8)
        fa = cgutils.create_struct_proxy(arr)(context, builder, value=arr_)
        builder.call(getItem, [flatbuffer, index, builder.bitcast(fa.ptr, i8p),
                               fa.sz, fa.is_null, element_size])

    return sig, codegen


@extending.intrinsic
def heavydb_column_array_is_null_(typingctx, x, i):
    sig = nb_types.boolean(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(i1, [i8p, i64])
        isNull = cgutils.get_or_insert_function(builder.module, fnty,
                                                "ColumnArray_isNull")
        flatbuffer = builder.extract_value(col, [0])
        return builder.call(isNull, [flatbuffer, index])

    return sig, codegen


@extending.intrinsic
def heavydb_column_array_set_null_(typingctx, x, i):
    sig = nb_types.void(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(i1, [i8p, i64])
        setNull = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnArray_setNull")
        flatbuffer = builder.extract_value(col, [0])
        builder.call(setNull, [flatbuffer, index])

    return sig, codegen


@extending.intrinsic
def deref(typingctx, x):
    sig = x.dtype(x)

    def codegen(context, builder, sig, args):
        [ptr] = args
        return builder.load(ptr)

    return sig, codegen


@extending.intrinsic
def heavydb_column_array_len_(typingctx, x):
    sig = nb_types.int64(x)

    def codegen(context, builder, sig, args):
        [col] = args
        return builder.extract_value(col, [1])

    return sig, codegen


@extending.overload(operator.getitem)
@extending.overload_method(ColumnArrayPointer, 'get_item')
def heavydb_column_array_getitem(x, i):
    if isinstance(x, ColumnArrayPointer):
        def impl(x, i):
            return heavydb_column_array_getitem_(deref(x), i)
        return impl
    elif isinstance(x, ColumnArrayType):
        def impl(x, i):
            return heavydb_column_array_getitem_(x, i)
        return impl


@extending.overload_method(ColumnArrayPointer, 'is_null')
@extending.overload_method(ColumnArrayType, 'is_null')
def heavydb_column_array_ptr_is_null(x, i):
    def impl(x, i):
        return heavydb_column_array_is_null_(deref(x), i)
    return impl


@extending.overload_method(ColumnArrayPointer, 'set_null')
@extending.overload_method(ColumnArrayType, 'set_null')
def heavydb_column_array_ptr_set_null(x, i):
    def impl(x, i):
        return heavydb_column_array_set_null_(deref(x), i)
    return impl


@extending.overload(operator.setitem)
@extending.overload_method(ColumnArrayPointer, 'set_item')
def heavydb_column_array_set_item(x, i, arr):
    if isinstance(x, ColumnArrayPointer):
        def impl(x, i, arr):
            return heavydb_column_array_setitem_(deref(x), i, arr)
        return impl
    elif isinstance(x, ColumnArrayType):
        def impl(x, i, arr):
            return heavydb_column_array_setitem_(x, i, arr)
        return impl


@extending.overload_method(ColumnArrayPointer, 'concat_item')
def heavydb_column_array_concat_item(x, i, arr):
    if isinstance(x, ColumnArrayPointer):
        def impl(x, i, arr):
            return heavydb_column_array_concatitem_(deref(x), i, arr)
        return impl
    elif isinstance(x, ColumnArrayType):
        def impl(x, i, arr):
            return heavydb_column_array_concatitem_(x, i, arr)
        return impl


@extending.overload(len)
def heavydb_column_array_len(x):
    if isinstance(x, ColumnArrayPointer):
        def impl(x):
            return heavydb_column_array_len_(deref(x))
        return impl
    elif isinstance(x, ColumnArrayType):
        def impl(x):
            return heavydb_column_array_len_(x)
        return impl
