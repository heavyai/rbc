
"""Implement Heavydb Column<GeoPoint> type support

Heavydb Column<GeoPoint> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['HeavyDBOutputColumnGeoPointType', 'HeavyDBColumnGeoPointType',
           'ColumnGeoPoint']

import operator
from typing import TypeVar
from rbc import typesystem
from .column import HeavyDBColumnType, HeavyDBOutputColumnType
from . import point2d
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
    def get_item(self, index: int) -> int:
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, point: 'point2d.Point2D') -> int:
        """
        ``self[index] = point``

        .. note::
            Only available on ``CPU``
        """

    def is_null(self, index: int) -> int:
        """
        Check if the GeoPoint at ``index`` is null.

        .. note::
            Only available on ``CPU``
        """

    def set_null(self, index: int) -> int:
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
    sig = Point2D(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        fa = cgutils.create_struct_proxy(sig.return_type)(context, builder)
        fnty = ir.FunctionType(fa._get_be_type(fa._datamodel), [i8p, i64, i32])
        getItem = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnGeoPoint_getItem")
        flatbuffer = builder.extract_value(col, [0])
        output_srid = i32(0)
        return builder.call(getItem, [flatbuffer, index, output_srid])

    return sig, codegen


@extending.intrinsic
def heavydb_column_setitem_(typingctx, x, i, point2d):
    sig = nb_types.void(x, i, point2d)

    def codegen(context, builder, sig, args):
        col, index, point2d_ = args
        fnty = ir.FunctionType(void, [i8p, i64, i8p])
        getItem = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnGeoPoint_setItem")
        flatbuffer = builder.extract_value(col, [0])
        fa = cgutils.create_struct_proxy(point2d)(context, builder, value=point2d_)
        builder.call(getItem, [flatbuffer, index, builder.bitcast(fa._getpointer(), i8p)])

    return sig, codegen


@extending.intrinsic
def heavydb_column_is_null_(typingctx, x, i):
    sig = nb_types.boolean(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(i1, [i8p, i64])
        isNull = cgutils.get_or_insert_function(builder.module, fnty,
                                                "ColumnGeoPoint_isNull")
        flatbuffer = builder.extract_value(col, [0])
        return builder.call(isNull, [flatbuffer, index])

    return sig, codegen


@extending.intrinsic
def heavydb_column_set_null_(typingctx, x, i):
    sig = nb_types.void(x, i)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(i1, [i8p, i64])
        setNull = cgutils.get_or_insert_function(builder.module, fnty,
                                                 "ColumnGeoPoint_setNull")
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
            return heavydb_column_getitem_(deref(x), i)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i):
            return heavydb_column_getitem_(x, i)
        return impl


@extending.overload_method(ColumnGeoPointPointer, 'is_null')
@extending.overload_method(ColumnGeoPointType, 'is_null')
def heavydb_column_ptr_is_null(x, i):
    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i):
            return heavydb_column_is_null_(deref(x), i)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i):
            return heavydb_column_is_null_(x, i)
        return impl


@extending.overload_method(ColumnGeoPointPointer, 'set_null')
@extending.overload_method(ColumnGeoPointType, 'set_null')
def heavydb_column_set_null(x, i):
    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i):
            return heavydb_column_set_null_(deref(x), i)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i):
            return heavydb_column_set_null_(x, i)
        return impl


@extending.overload(operator.setitem)
@extending.overload_method(ColumnGeoPointPointer, 'set_item')
def heavydb_column_set_item(x, i, point):
    if isinstance(x, ColumnGeoPointPointer):
        def impl(x, i, point):
            return heavydb_column_setitem_(deref(x), i, point)
        return impl
    elif isinstance(x, ColumnGeoPointType):
        def impl(x, i, point):
            return heavydb_column_setitem_(x, i, point)
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
