"""Implement Heavydb Column<GeoMultiPoint> type support

Heavydb Column<GeoMultiPoint> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoMultiPointType",
    "HeavyDBColumnGeoMultiPointType",
    "ColumnGeoMultiPoint",
]

from . import geomultipoint
from .column_flatbuffer import (ColumnFlatBuffer, HeavyDBColumnFlatBufferType,
                                HeavyDBOutputColumnFlatBufferType)


class ColumnGeoMultiPoint(ColumnFlatBuffer):
    """
    RBC ``Column<GeoMultiPoint>`` type that corresponds to HeavyDB
    ``COLUMN<MULTIPOINT>``

    In HeavyDB, a Column of type ``GeoMultiPoint`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }
    """

    def __getitem__(self, index: int) -> 'geomultipoint.GeoMultiPoint':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def get_item(self, index: int) -> 'geomultipoint.GeoMultiPoint':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, buf: 'geomultipoint.GeoMultiPoint') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """

    def __setitem__(self, index: int, buf: 'geomultipoint.GeoMultiPoint') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """


class HeavyDBColumnGeoMultiPointType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def type_name(self):
        return "GeoMultiPoint"


class HeavyDBOutputColumnGeoMultiPointType(
    HeavyDBColumnGeoMultiPointType, HeavyDBOutputColumnFlatBufferType
):
    """ """
