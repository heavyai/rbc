"""RBC GeoMultiPolygon type that corresponds to HeavyDB type GeoMultiPolygon.
"""

__all__ = ["HeavyDBGeoMultiPolygonType", "GeoMultiPolygon"]

from typing import List

from numba.core import extending
from numba.core import types as nb_types

from . import geopolygon
from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec3,
                               heavydb_geo_toCoords_vec3)


class GeoMultiPolygonNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoMultiPolygonType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoMultiPolygonNumbaType

    @property
    def type_name(self):
        return "GeoMultiPolygon"

    @property
    def item_type(self):
        return "GeoPolygon"


class GeoMultiPolygon(GeoNestedArray):
    """
    RBC ``GeoMultiPolygon`` type that corresponds to HeavyDB type
    ``MULTIPOLYGON``.

    .. code-block:: c

        struct MultiPolygon {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """

    def __getitem__(self, index: int) -> 'geopolygon.GeoPolygon':
        """
        Return the ``POLYGON`` at given index
        """

    def get_item(self, index: int) -> 'geopolygon.GeoPolygon':
        """
        Return the ``POLYGON`` at given index
        """

    def to_coords(self) -> List[List[List[float]]]:
        """
        .. note::
            Only available on ``CPU``
        """

    def from_coords(self, coords: List[List[List[float]]]) -> None:
        """
        .. note::
            Only available on ``CPU``
        """


@extending.overload_method(GeoMultiPolygonNumbaType, "from_coords")
def heavydb_geomultipolygon_fromCoords(geo, lst):
    if isinstance(geo, GeoMultiPolygonNumbaType) and isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec3(geo, lst)


@extending.overload_method(GeoMultiPolygonNumbaType, "to_coords")
def heavydb_geolinestring_toCoords(geo):
    return heavydb_geo_toCoords_vec3(geo)
