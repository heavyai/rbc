"""Implement Heavydb Column<GeoMultiPolygon> type support

Heavydb Column<GeoMultiPolygon> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoMultiPolygonType",
    "HeavyDBColumnGeoMultiPolygonType",
    "ColumnGeoMultiPolygon",
]

from . import geomultipolygon
from .column_flatbuffer import (ColumnFlatBuffer, HeavyDBColumnFlatBufferType,
                                HeavyDBOutputColumnFlatBufferType)


class ColumnGeoMultiPolygon(ColumnFlatBuffer):
    """
    RBC ``Column<GeoMultiPolygon>`` type that corresponds to HeavyDB
    ``COLUMN<MULTIPOLYGON>``

    In HeavyDB, a Column of type ``GeoMultiPolygon`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """

    def __getitem__(self, index: int) -> 'geomultipolygon.GeoMultiPolygon':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def get_item(self, index: int) -> 'geomultipolygon.GeoMultiPolygon':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, buf: 'geomultipolygon.GeoMultiPolygon') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """

    def __setitem__(self, index: int, buf: 'geomultipolygon.GeoMultiPolygon') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
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
