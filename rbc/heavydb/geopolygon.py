"""RBC GeoPolygon type that corresponds to HeavyDB type GeoPolygon.
"""

__all__ = ["HeavyDBGeoPolygonType", "GeoPolygon"]

from numba.core import extending
from numba.core import types as nb_types

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
    RBC ``GeoPolygon`` type that corresponds to HeavyDB type GeoPolygon.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """


@extending.overload_method(GeoPolygonNumbaType, "from_coords")
def heavydb_geopolygon_fromCoords(geo, lst):
    if isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec2(geo, lst)


@extending.overload_method(GeoPolygonNumbaType, "to_coords")
def heavydb_geopolygon_toCoords(geo):
    return heavydb_geo_toCoords_vec2(geo)
