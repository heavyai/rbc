"""Implement Heavydb Column<GeoLineString> type support

Heavydb Column<GeoLineString> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoLineStringType",
    "HeavyDBColumnGeoLineStringType",
    "ColumnGeoLineString",
]

from .column_flatbuffer import (ColumnFlatBuffer, HeavyDBColumnFlatBufferType,
                                HeavyDBOutputColumnFlatBufferType)


class ColumnGeoLineString(ColumnFlatBuffer):
    """
    RBC ``Column<GeoLineString>`` type that corresponds to HeavyDB
    ``COLUMN<LINESTRING>``

    In HeavyDB, a Column of type ``GeoLineString`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """


class HeavyDBColumnGeoLineStringType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def type_name(self):
        return "GeoLineString"


class HeavyDBOutputColumnGeoLineStringType(
    HeavyDBColumnGeoLineStringType, HeavyDBOutputColumnFlatBufferType
):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoLineString> is the same as Column<GeoLineString> but introduced to
    distinguish the input and output arguments of UDTFs.
    """
