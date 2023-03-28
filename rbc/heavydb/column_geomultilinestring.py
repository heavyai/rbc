"""Implement Heavydb Column<GeoMultiLineString> type support

Heavydb Column<GeoMultiLineString> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoMultiLineStringType",
    "HeavyDBColumnGeoMultiLineStringType",
    "ColumnGeoMultiLineString",
]

from .column_flatbuffer import (ColumnFlatBuffer, HeavyDBColumnFlatBufferType,
                                HeavyDBOutputColumnFlatBufferType)


class ColumnGeoMultiLineString(ColumnFlatBuffer):
    """
    RBC ``Column<GeoMultiLineString>`` type that corresponds to HeavyDB
    ``COLUMN<MULTILINESTRING>``

    In HeavyDB, a Column of type ``GeoMultiLineString`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """


class HeavyDBColumnGeoMultiLineStringType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def type_name(self):
        return "GeoMultiLineString"


class HeavyDBOutputColumnGeoMultiLineStringType(
    HeavyDBColumnGeoMultiLineStringType, HeavyDBOutputColumnFlatBufferType
):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<GeoMultiLineString> is the same as Column<GeoMultiLineString> but introduced to
    distinguish the input and output arguments of UDTFs.
    """
