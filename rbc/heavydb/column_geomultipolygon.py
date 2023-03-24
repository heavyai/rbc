"""Implement Heavydb Column<GeoMultiPolygon> type support

Heavydb Column<GeoMultiPolygon> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoMultiPolygonType",
    "HeavyDBColumnGeoMultiPolygonType",
    "ColumnGeoMultiPolygon",
]

from .column_flatbuffer import (
    ColumnFlatBuffer,
    HeavyDBColumnFlatBufferType,
    HeavyDBOutputColumnFlatBufferType,
)


class ColumnGeoMultiPolygon(ColumnFlatBuffer):
    """
    RBC ``Column<GeoMultiPolygon>`` type that corresponds to HeavyDB COLUMN<GeoMultiPolygon>

    In HeavyDB, a Column of type ``GeoMultiPolygon`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """


class HeavyDBColumnGeoMultiPolygonType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def type_name(self):
        return "GeoMultiPolygon"


class HeavyDBOutputColumnGeoMultiPolygonType(
    HeavyDBColumnGeoMultiPolygonType, HeavyDBOutputColumnFlatBufferType
):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoMultiPolygon> is the same as Column<GeoMultiPolygon> but introduced to
    distinguish the input and output arguments of UDTFs.
    """
