"""RBC GeoMultiPolygon type that corresponds to HeavyDB type GeoMultiPolygon.
"""

__all__ = ["HeavyDBGeoMultiPolygonType", "GeoMultiPolygon"]

from numba.core import extending
from numba.core import types as nb_types

from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec3)


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
    RBC ``GeoMultiPolygon`` type that corresponds to HeavyDB type GeoMultiPolygon.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """


@extending.overload_method(GeoMultiPolygonNumbaType, "fromCoords")
def heavydb_geo_fromCoords(geo, lst):
    if isinstance(geo, GeoMultiPolygonNumbaType) and isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec3(geo, lst)
