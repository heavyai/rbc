"""RBC GeoPolygon type that corresponds to HeavyDB type GeoPolygon.
"""

__all__ = ["HeavyDBGeoPolygonType", "GeoPolygon"]

from typing import List

from numba.core import extending
from numba.core import types as nb_types

from . import geolinestring
from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec2,
                               heavydb_geo_toCoords_vec2)


class GeoPolygonNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoPolygonType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoPolygonNumbaType

    @property
    def type_name(self):
        return "GeoPolygon"

    @property
    def item_type(self):
        return "GeoLineString"


class GeoPolygon(GeoNestedArray):
    """
    RBC ``GeoPolygon`` type that corresponds to HeavyDB type ``POLYGON``.

    .. code-block:: c

        struct Polygon {
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


@extending.overload_method(GeoPolygonNumbaType, "from_coords")
def heavydb_geopolygon_fromCoords(geo, lst):
    if isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec2(geo, lst)


@extending.overload_method(GeoPolygonNumbaType, "to_coords")
def heavydb_geopolygon_toCoords(geo):
    return heavydb_geo_toCoords_vec2(geo)


@extending.overload_method(GeoPolygonNumbaType, "n_rings")
def heavydb_geopolygon_nrings(geo):
    # In GeoPolygon and GeoMultiLineString, len(geo) gives the number of rings
    def impl(geo):
        return len(geo)
    return impl
