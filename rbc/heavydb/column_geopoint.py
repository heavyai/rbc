
"""Implement Heavydb Column<GeoPoint> type support

Heavydb Column<GeoPoint> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['HeavyDBOutputColumnGeoPointType', 'HeavyDBColumnGeoPointType',
           'ColumnGeoPoint']

import operator
from typing import TypeVar
from rbc import typesystem
from rbc.external import external
from .column import HeavyDBColumnType, HeavyDBOutputColumnType
from . import point2d
from .utils import as_voidptr, get_alloca, deref
from .metatype import HeavyDBMetaType
from numba.core import extending, cgutils
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


class ColumnGeoPoint(metaclass=HeavyDBMetaType):
    """
    RBC ``Column<GeoPoint>`` type that corresponds to HeavyDB COLUMN<GEOPOINT>

    In HeavyDB, a Column of type ``GeoPoint`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """
    def get_item(self, index: int) -> 'point2d.Point2D':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, point: 'point2d.Point2D') -> None:
        """
        ``self[index] = point``

        .. note::
            Only available on ``CPU``
        """

    def is_null(self, index: int) -> bool:
        """
        Check if the GeoPoint at ``index`` is null.

        .. note::
            Only available on ``CPU``
        """

    def set_null(self, index: int) -> None:
        """
        Set the GeoPoint at ``index`` to null.

        .. note::
            Only available on ``CPU``
        """

    def __len__(self) -> int:
        """
        Return the length of the Column<GeoPoint>.

        ..  note::
             Only available on ``CPU``
        """


class HeavyDBColumnGeoPointType(HeavyDBColumnType):

    def postprocess_type(self):
        return self.params(shorttypename='Column')

    @property
    def numba_type(self):
        return ColumnGeoPointType

    @property
    def numba_pointer_type(self):
        return ColumnGeoPointPointer

    @property
    def element_type(self):
        return typesystem.Type.fromstring('int8_t')


class HeavyDBOutputColumnGeoPointType(HeavyDBColumnGeoPointType, HeavyDBOutputColumnType):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoPoint> is the same as Column<GeoPoint> but introduced to
    distinguish the input and output arguments of UDTFs.
    """


class ColumnGeoPointType(nb_types.Type):
    def __init__(self, dtype):
        self.dtype = 'GeoPoint'  # struct dtype
        name = "Column<GeoPoint>"
        super().__init__(name)

    @property
    def key(self):
        return self.dtype

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.dtype  # GeoPoint


class ColumnGeoPointPointer(nb_types.Type):
    """Numba type class for pointers to HeavyDB buffer structures.

    We are not deriving from CPointer because ColumnGeoPointPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        self.eltype = dtype.eltype  # buffer element dtype
        name = "Column<GeoPoint>*"
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


@extending.intrinsic
def heavydb_column_getitem_(typingctx, x, i):
    Point2D = typesystem.Type.fromstring('Point2D').tonumba()
    # oddly enough, a Column<GeoPoint> returns a Point2D
    sig = Point2D(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        # fnty = ir.FunctionType(fa._get_be_type(fa._datamodel), [i8p, i64, i32, i8p])
        fnty = ir.FunctionType(void, [i8p, i64, i32, i8p])
        getItem = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnGeoPoint_getItem")
        output_srid = i32(0)
        col_ptr = builder.bitcast(col, i8p)

        # Alloca Point2D
        fa = context.make_helper(builder, sig.return_type)

        # Point2D -> void*
        result_ptr = builder.bitcast(fa._getpointer(), i8p)

        # call func
        builder.call(getItem, [col_ptr, index, output_srid, result_ptr])

        # convert void* -> Point2D
        point_type = fa._get_be_type(fa._datamodel)
        return builder.load(builder.bitcast(result_ptr, point_type.as_pointer()))

    return sig, codegen


@extending.intrinsic
def heavydb_column_len_(typingctx, x):
    sig = nb_types.int64(x)

    def codegen(context, builder, sig, args):
        [col] = args
        return builder.extract_value(col, [1])

    return sig, codegen


@extending.overload(operator.getitem)
@extending.overload_method(ColumnGeoPointPointer, 'get_item')
def heavydb_column_getitem(x, i):
    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i):
            return heavydb_column_getitem_(x, i)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i):
            return heavydb_column_getitem_(x, i)
        return impl


@extending.overload_method(ColumnGeoPointPointer, 'is_null')
@extending.overload_method(ColumnGeoPointType, 'is_null')
def heavydb_column_ptr_is_null(x, i):
    isNull = external('bool ColumnGeoPoint_isNull(int8_t*, int64_t)|cpu')

    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i):
            return isNull(as_voidptr(x), i)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i):
            return isNull(get_alloca(x), i)
        return impl


@extending.overload_method(ColumnGeoPointPointer, 'set_null')
@extending.overload_method(ColumnGeoPointType, 'set_null')
def heavydb_column_set_null(x, i):
    setNull = external('void ColumnGeoPoint_setNull(int8_t*, int64_t)|cpu')

    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i):
            return setNull(as_voidptr(x), i)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i):
            return setNull(get_alloca(x), i)
        return impl


@extending.overload(operator.setitem)
@extending.overload_method(ColumnGeoPointPointer, 'set_item')
def heavydb_column_set_item(x, i, point):
    setItem = external('void ColumnGeoPoint_setItem(int8_t*, int64_t, int8_t*)|cpu')

    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i, point):
            return setItem(as_voidptr(x), i, as_voidptr(get_alloca(point)))
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i, point):
            return setItem(get_alloca(x), i, as_voidptr(get_alloca(point)))
        return impl


@extending.overload(len)
def heavydb_column_len(x):
    if isinstance(x, ColumnGeoPointPointer):
        def impl(x):
            return heavydb_column_len_(deref(x))
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x):
            return heavydb_column_len_(x)
        return impl
