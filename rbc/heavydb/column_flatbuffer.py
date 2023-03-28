"""
Base class for Column<Geo*> types
"""

__all__ = [
    "HeavyDBOutputColumnFlatBufferType",
    "HeavyDBColumnFlatBufferType",
    "ColumnFlatBuffer",
    "ColumnFlatBufferPointer",
    "ColumnFlatBufferType",
]

import operator

from llvmlite import ir
from numba.core import extending, cgutils
from numba.core import types as nb_types

from rbc import typesystem
from rbc.external import external

from typing import TypeVar

from .column import HeavyDBColumnType, HeavyDBOutputColumnType
from .metatype import HeavyDBMetaType
from .utils import as_voidptr, deref, get_alloca

i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()

T = TypeVar("T")


class ColumnFlatBuffer(metaclass=HeavyDBMetaType):
    """
    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """

    def is_null(self, index: int) -> bool:
        """
        Check if element at ``index`` is null.

        .. note::
            Only available on ``CPU``
        """

    def set_null(self, index: int) -> None:
        """
        Set element at ``index`` to null.

        .. note::
            Only available on ``CPU``
        """

    def get_n_of_values(self) -> int:
        """
        Return the total number of points in a Column[T]

        .. note::
            Only available on ``CPU``
        """

    def size(self) -> int:
        """
        Return the length of the Column[T]

        ..  note::
             Only available on ``CPU``
        """

    def __len__(self) -> int:
        """
        Return the length of the Column[T]

        ..  note::
             Only available on ``CPU``
        """


class HeavyDBColumnFlatBufferType(HeavyDBColumnType):
    def postprocess_type(self):
        return self.params(shorttypename="Column")

    @property
    def numba_type(self):
        return ColumnFlatBufferType

    @property
    def numba_pointer_type(self):
        return ColumnFlatBufferPointer

    @property
    def type_name(self):
        raise NotImplementedError()

    @property
    def element_type(self):
        return typesystem.Type.fromstring("int8_t")

    @property
    def custom_params(self):
        return {
            **super().custom_params,
            "name": f"Column<{self.type_name}>",
        }


class HeavyDBOutputColumnFlatBufferType(
    HeavyDBColumnFlatBufferType, HeavyDBOutputColumnType
):
    """ """


class ColumnFlatBufferType(nb_types.Type):
    def __init__(self, name):
        self.dtype = name[7:-1]
        super().__init__(name)

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.dtype


class ColumnFlatBufferPointer(nb_types.Type):
    """Numba type class for pointers to HeavyDB buffer structures."""

    def __init__(self, dtype):
        self.dtype = dtype  # struct dtype
        self.eltype = dtype.eltype  # buffer element dtype
        super().__init__(f"Column<{dtype.eltype}>*")


@extending.intrinsic
def heavydb_column_len_(typingctx, col):
    sig = nb_types.int64(col)

    def codegen(context, builder, sig, args):
        [col] = args
        return builder.extract_value(col, [1])

    return sig, codegen


@extending.intrinsic
def heavydb_column_getitem_(typingctx, col, index):
    eltype = col.eltype
    LS = typesystem.Type.fromstring(eltype).tonumba()
    sig = LS(col, index)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(void, [i8p, i64, i8p])
        getItem = cgutils.get_or_insert_function(
            builder.module, fnty, f"Column{eltype}_getItem"
        )
        col_ptr = builder.bitcast(col, i8p)

        # Alloca Geo<T>
        fa = context.make_helper(builder, sig.return_type)
        fa.n_ = fa.n_.type(1)

        # Geo<T> -> void*
        result_ptr = builder.bitcast(fa._getpointer(), i8p)

        # call func
        builder.call(getItem, [col_ptr, index, result_ptr])

        # convert void* -> Geo<T>
        point_type = fa._get_be_type(fa._datamodel)
        return builder.load(builder.bitcast(result_ptr, point_type.as_pointer()))

    return sig, codegen


@extending.overload_method(ColumnFlatBufferPointer, "get_n_of_values")
def heavydb_column_getnofvalues(col):
    getNofValues = external(f"int64_t Column{col.eltype}_getNofValues(int8_t*)|cpu")

    if isinstance(col, ColumnFlatBufferPointer):

        def impl(col):
            return getNofValues(as_voidptr(col))

        return impl


@extending.overload(operator.getitem)
@extending.overload_method(ColumnFlatBufferPointer, "get_item")
def heavydb_column_getitem(col, index):
    if isinstance(col, ColumnFlatBufferPointer) and col.eltype != "GeoPoint":

        def impl(col, index):
            return heavydb_column_getitem_(col, index)

        return impl


@extending.overload_method(ColumnFlatBufferPointer, "is_null")
def heavydb_column_ptr_is_null(col, index):
    isNull = external(f"bool Column{col.eltype}_isNull(int8_t*, int64_t)|cpu")

    if isinstance(col, ColumnFlatBufferPointer):

        def impl(col, index):
            return isNull(as_voidptr(col), index)

        return impl


@extending.overload_method(ColumnFlatBufferPointer, "set_null")
def heavydb_column_set_null(col, index):
    setNull = external(f"bool Column{col.eltype}_setNull(int8_t*, int64_t)|cpu")
    if isinstance(col, ColumnFlatBufferPointer):

        def impl(col, index):
            setNull(as_voidptr(col), index)

        return impl


@extending.overload(operator.setitem)
@extending.overload_method(ColumnFlatBufferPointer, "set_item")
def heavydb_column_set_item(col, index, rhs):
    if isinstance(col, ColumnFlatBufferPointer):
        sig = f"void Column{col.eltype}_setItem(int8_t*, int64_t, int8_t* rhs)"
        setItem = external(sig)

        def impl(col, index, rhs):
            setItem(as_voidptr(col), index, as_voidptr(get_alloca(rhs)))

        return impl


@extending.overload(len)
def heavydb_column_len(col):
    if isinstance(col, ColumnFlatBufferPointer):

        def impl(col):
            return heavydb_column_len_(deref(col))

        return impl
