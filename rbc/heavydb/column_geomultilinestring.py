"""Implement Heavydb Column<GeoMultiLineString> type support

Heavydb Column<GeoMultiLineString> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnGeoMultiLineStringType",
    "HeavyDBColumnGeoMultiLineStringType",
    "ColumnGeoMultiLineString",
]

from . import geomultilinestring
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

    def __getitem__(self, index: int) -> 'geomultilinestring.GeoMultiLineString':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def get_item(self, index: int) -> 'geomultilinestring.GeoMultiLineString':
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, buf: 'geomultilinestring.GeoMultiLineString') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """

    def __setitem__(self, index: int, buf: 'geomultilinestring.GeoMultiLineString') -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
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
