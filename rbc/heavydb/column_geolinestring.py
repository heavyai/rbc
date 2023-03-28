"""Implement Heavydb Column<GeoLineString> type support

Heavydb Column<GeoLineString> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoLineStringType",
    "HeavyDBColumnGeoLineStringType",
    "ColumnGeoLineString",
]

from . import geolinestring
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

    def __getitem__(self, index: int) -> 'geolinestring.GeoLineString':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def get_item(self, index: int) -> 'geolinestring.GeoLineString':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, buf: 'geolinestring.GeoLineString') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """

    def __setitem__(self, index: int, buf: 'geolinestring.GeoLineString') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
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
