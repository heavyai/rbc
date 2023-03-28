"""Implement Heavydb Column<GeoPolygon> type support

Heavydb Column<GeoPolygon> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoPolygonType",
    "HeavyDBColumnGeoPolygonType",
    "ColumnGeoPolygon",
]

from . import geopolygon
from .column_flatbuffer import (ColumnFlatBuffer, HeavyDBColumnFlatBufferType,
                                HeavyDBOutputColumnFlatBufferType)


class ColumnGeoPolygon(ColumnFlatBuffer):
    """
    RBC ``Column<GeoPolygon>`` type that corresponds to HeavyDB
    ``COLUMN<POLYGON>``

    In HeavyDB, a Column of type ``GeoPolygon`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """

    def __getitem__(self, index: int) -> 'geopolygon.GeoPolygon':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def get_item(self, index: int) -> 'geopolygon.GeoPolygon':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, buf: 'geopolygon.GeoPolygon') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """

    def __setitem__(self, index: int, buf: 'geopolygon.GeoPolygon') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
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
