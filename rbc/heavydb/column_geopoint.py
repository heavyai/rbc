"""Implement Heavydb Column<GeoPoint> type support

Heavydb Column<GeoPoint> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoPointType",
    "HeavyDBColumnGeoPointType",
    "ColumnGeoPoint",
]

import operator

from llvmlite import ir
from numba.core import cgutils, extending

from rbc import typesystem

from .column_flatbuffer import (
    ColumnFlatBuffer,
    ColumnFlatBufferPointer,
    ColumnFlatBufferType,
    HeavyDBColumnFlatBufferType,
    HeavyDBOutputColumnFlatBufferType,
)
from .metatype import HeavyDBMetaType


i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()


class ColumnGeoPoint(ColumnFlatBuffer, metaclass=HeavyDBMetaType):
    """
    RBC ``Column<GeoPoint>`` type that corresponds to HeavyDB COLUMN<GeoPoint>

    In HeavyDB, a Column of type ``GeoPoint`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """


class HeavyDBColumnGeoPointType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def numba_type(self):
        return ColumnGeoPointType

    @property
    def numba_pointer_type(self):
        return ColumnGeoPointPointer

    @property
    def type_name(self):
        return "GeoPoint"


class HeavyDBOutputColumnGeoPointType(
    HeavyDBColumnGeoPointType, HeavyDBOutputColumnFlatBufferType
):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoPoint> is the same as Column<GeoPoint> but introduced to
    distinguish the input and output arguments of UDTFs.
    """


class ColumnGeoPointType(ColumnFlatBufferType):
    pass


class ColumnGeoPointPointer(ColumnFlatBufferPointer):
    pass


@extending.intrinsic
def heavydb_column_getitem_(typingctx, col, index):
    Point2D = typesystem.Type.fromstring("Point2D").tonumba()
    # oddly enough, a Column<GeoPoint> returns a Point2D
    sig = Point2D(col, index)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(void, [i8p, i64, i32, i8p])
        getItem = cgutils.get_or_insert_function(
            builder.module, fnty, "ColumnGeoPoint_getItem"
        )
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


@extending.overload(operator.getitem)
@extending.overload_method(ColumnGeoPointPointer, "get_item")
def heavydb_column_getitem(col, index):
    if isinstance(col, ColumnGeoPointPointer):

        def impl(col, index):
            return heavydb_column_getitem_(col, index)

        return impl
