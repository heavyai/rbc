"""Implement Heavydb Column<GeoPolygon> type support

Heavydb Column<GeoPolygon> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoPolygonType",
    "HeavyDBColumnGeoPolygonType",
    "ColumnGeoPolygon",
]

from .column_flatbuffer import (
    ColumnFlatBuffer,
    HeavyDBColumnFlatBufferType,
    HeavyDBOutputColumnFlatBufferType,
)


class ColumnGeoPolygon(ColumnFlatBuffer):
    """
    RBC ``Column<GeoPolygon>`` type that corresponds to HeavyDB COLUMN<GeoPolygon>

    In HeavyDB, a Column of type ``GeoPolygon`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """


class HeavyDBColumnGeoPolygonType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def type_name(self):
        return "GeoPolygon"


class HeavyDBOutputColumnGeoPolygonType(
    HeavyDBColumnGeoPolygonType, HeavyDBOutputColumnFlatBufferType
):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoPolygon> is the same as Column<GeoPolygon> but introduced to
    distinguish the input and output arguments of UDTFs.
    """
