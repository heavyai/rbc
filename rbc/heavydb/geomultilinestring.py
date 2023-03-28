"""RBC GeoMultiLineString type that corresponds to HeavyDB type GeoMultiLineString.
"""

__all__ = ["HeavyDBGeoMultiLineStringType", "GeoMultiLineString"]


from numba.core import extending
from numba.core import types as nb_types

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
    RBC ``GeoMultiLineString`` type that corresponds to HeavyDB type GeoMultiLineString.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """


@extending.overload_method(GeoMultiLineStringNumbaType, "from_coords")
def heavydb_geomultilinestring_fromCoords(geo, lst):
    if isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec2(geo, lst)


@extending.overload_method(GeoMultiLineStringNumbaType, "to_coords")
def heavydb_geomultilinestring_toCoords(geo):
    return heavydb_geo_toCoords_vec2(geo)
