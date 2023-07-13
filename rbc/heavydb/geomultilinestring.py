"""RBC GeoMultiLineString type that corresponds to HeavyDB type GeoMultiLineString.
"""

__all__ = ["HeavyDBGeoMultiLineStringType", "GeoMultiLineString"]


from typing import List

from numba.core import extending
from numba.core import types as nb_types

from . import geolinestring
from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec2,
                               heavydb_geo_toCoords_vec2)


class GeoMultiLineStringNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoMultiLineStringType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoMultiLineStringNumbaType

    @property
    def type_name(self):
        return "GeoMultiLineString"

    @property
    def item_type(self):
        return "GeoLineString"


class GeoMultiLineString(GeoNestedArray):
    """
    RBC ``GeoMultiLineString`` type that corresponds to HeavyDB type
    ``MULTILINESTRING``.

    .. code-block:: c

        struct MultiLineString {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
    def __getitem__(self, index: int) -> 'geolinestring.GeoLineString':
        """
        Return the ``LINESTRING`` at given index
        """

    def get_item(self, index: int) -> 'geolinestring.GeoLineString':
        """
        Return the ``LINESTRING`` at given index
        """

    def to_coords(self) -> List[List[float]]:
        """
        .. note::
            Only available on ``CPU``
        """

    def from_coords(self, coords: List[List[float]]) -> None:
        """
        .. note::
            Only available on ``CPU``
        """


@extending.overload_method(GeoMultiLineStringNumbaType, "from_coords")
def heavydb_geomultilinestring_fromCoords(geo, lst):
    if isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec2(geo, lst)


@extending.overload_method(GeoMultiLineStringNumbaType, "to_coords")
def heavydb_geomultilinestring_toCoords(geo):
    return heavydb_geo_toCoords_vec2(geo)


@extending.overload_method(GeoMultiLineStringNumbaType, "n_rings")
def heavydb_geomultilinestring_nrings(geo):
    # In GeoPolygon and GeoMultiLineString, len(geo) gives the number of rings
    def impl(geo):
        return len(geo)
    return impl
