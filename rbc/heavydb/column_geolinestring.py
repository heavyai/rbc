
"""Implement Heavydb Column<GeoLineString> type support

Heavydb Column<GeoLineString> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['HeavyDBOutputColumnGeoLineStringType', 'HeavyDBColumnGeoLineStringType',
           'ColumnGeoLineString']

import operator
from typing import TypeVar
from rbc import typesystem
from rbc.external import external
from .column import HeavyDBColumnType, HeavyDBOutputColumnType
from .metatype import HeavyDBMetaType
from . import geolinestring
from .utils import as_voidptr, deref, get_alloca
from numba.core import extending
from numba.core import types as nb_types
from llvmlite import ir


T = TypeVar('T')


i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()


class ColumnGeoLineString(metaclass=HeavyDBMetaType):
    """
    RBC ``Column<GeoLineString>`` type that corresponds to HeavyDB COLUMN<GEOPOINT>

    In HeavyDB, a Column of type ``GeoLineString`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """
    def get_item(self, index: int) -> 'geolinestring.GeoLineString':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """
        pass

    def set_item(self, index: int, buf: 'geolinestring.GeoLineString') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """
        pass

    def is_null(self, index: int) -> bool:
        """
        Check if the GeoLineString at ``index`` is null.

        .. note::
            Only available on ``CPU``
        """
        pass

    def set_null(self, index: int) -> None:
        """
        Set the GeoLineString at ``index`` to null.

        .. note::
            Only available on ``CPU``
        """
        pass

    def get_n_of_values(self) -> int:
        """
        Return the total number of points in a Column<GeoLineString>

        .. note::
            Only available on ``CPU``
        """
        pass

    def __len__(self) -> int:
        """
        Return the length of the Column<GeoLineString>.

        ..  note::
             Only available on ``CPU``
        """
        pass


class HeavyDBColumnGeoLineStringType(HeavyDBColumnType):

    def postprocess_type(self):
        return self.params(shorttypename='Column')

    @property
    def numba_type(self):
        return ColumnGeoLineStringType

    @property
    def numba_pointer_type(self):
        return ColumnGeoLineStringPointer

    @property
    def element_type(self):
        return typesystem.Type.fromstring('int8_t')


class HeavyDBOutputColumnGeoLineStringType(HeavyDBColumnGeoLineStringType,
                                           HeavyDBOutputColumnType):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoLineString> is the same as Column<GeoLineString> but introduced to
    distinguish the input and output arguments of UDTFs.
    """


class ColumnGeoLineStringType(nb_types.Type):
    def __init__(self, dtype):
        self.dtype = 'GeoLineString'  # struct dtype
        name = "Column<GeoLineString>"
        super().__init__(name)

    @property
    def key(self):
        return self.dtype

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.dtype  # GeoLineString


class ColumnGeoLineStringPointer(nb_types.Type):
    """Numba type class for pointers to HeavyDB buffer structures.

    We are not deriving from CPointer because ColumnGeoLineStringPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        self.eltype = dtype.eltype  # buffer element dtype
        name = "Column<GeoLineString>*"
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


@extending.intrinsic
def heavydb_column_len_(typingctx, x):
    sig = nb_types.int64(x)

    def codegen(context, builder, sig, args):
        [col] = args
        return builder.extract_value(col, [1])

    return sig, codegen


@extending.overload_method(ColumnGeoLineStringPointer, 'getNofValues')
def heavydb_column_getnofvalues(x):
    getNofValues = external('int64_t ColumnGeoLineString_getNofValues(int8_t*)|cpu')

    if isinstance(x, ColumnGeoLineStringPointer):
        def impl(x):
            return getNofValues(as_voidptr(x))
        return impl


@extending.overload(operator.getitem)
@extending.overload_method(ColumnGeoLineStringPointer, 'get_item')
def heavydb_column_getitem(x, i):

    if isinstance(x, ColumnGeoLineStringPointer):
        def impl(x, i):
            obj = geolinestring.GeoLineString(as_voidptr(x), i)
            return obj
        return impl


@extending.overload_method(ColumnGeoLineStringPointer, 'is_null')
@extending.overload_method(ColumnGeoLineStringType, 'is_null')
def heavydb_column_ptr_is_null(x, i):
    isNull = external('bool ColumnGeoLineString_isNull(int8_t*, int64_t)|cpu')

    if isinstance(x, ColumnGeoLineStringPointer):
        def impl(x, i):
            return isNull(as_voidptr(x), i)
        return impl


@extending.overload_method(ColumnGeoLineStringPointer, 'set_null')
@extending.overload_method(ColumnGeoLineStringType, 'set_null')
def heavydb_column_set_null(x, i):
    setNull = external('bool ColumnGeoLineString_setNull(int8_t*, int64_t)|cpu')
    if isinstance(x, ColumnGeoLineStringPointer):
        def impl(x, i):
            setNull(as_voidptr(x), i)
        return impl


@extending.overload(operator.setitem)
@extending.overload_method(ColumnGeoLineStringPointer, 'set_item')
def heavydb_column_set_item(x, i, rhs):
    setItem = external('void ColumnGeoLineString_setItem(int8_t*, int64_t, int8_t* column_ptr, int64_t index)')
    if isinstance(x, ColumnGeoLineStringPointer):
        def impl(x, i, rhs):
            setItem(as_voidptr(x), i, rhs.column_ptr_, rhs.index_)
        return impl


@extending.overload(len)
def heavydb_column_len(x):
    if isinstance(x, ColumnGeoLineStringPointer):
        def impl(x):
            return heavydb_column_len_(deref(x))
        return impl