"""Implement Heavydb Column<GeoMultiPoint> type support

Heavydb Column<GeoMultiPoint> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoMultiPointType",
    "HeavyDBColumnGeoMultiPointType",
    "ColumnGeoMultiPoint",
]

from .column_flatbuffer import (
    ColumnFlatBuffer,
    HeavyDBColumnFlatBufferType,
    HeavyDBOutputColumnFlatBufferType,
)


class ColumnGeoMultiPoint(ColumnFlatBuffer):
    """ """


class HeavyDBColumnGeoMultiPointType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def type_name(self):
        return "GeoMultiPoint"


class HeavyDBOutputColumnGeoMultiPointType(
    HeavyDBColumnGeoMultiPointType, HeavyDBOutputColumnFlatBufferType
):
    """ """
