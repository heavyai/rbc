"""RBC GeoMultiPoint type that corresponds to HeavyDB type GeoMultiPoint.
"""

__all__ = ["HeavyDBGeoMultiPointType", "GeoMultiPoint"]

from numba.core import extending
from numba.core import types as nb_types

from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec,
                               heavydb_geo_toCoords_vec)


class GeoMultiPointNumbaType(GeoNestedArrayNumbaType):
    def __init__(self):
        super().__init__(name="GeoMultiPointNumbaType")


class HeavyDBGeoMultiPointType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoMultiPoint"

    @property
    def item_type(self):
        return "Point2D"


class GeoMultiPoint(GeoNestedArray):
    """
    RBC ``GeoMultiPoint`` type that corresponds to HeavyDB type GeoMultiPoint.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """


@extending.overload_method(GeoMultiPointNumbaType, "toCoords")
def heavydb_geomultipoint_toCoords(geo):
    return heavydb_geo_toCoords_vec(geo)


@extending.overload_method(GeoMultiPointNumbaType, "fromCoords")
def heavydb_geomultipoint_fromCoords(geo, lst):
    if isinstance(geo, GeoMultiPointNumbaType) and isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec(geo, lst)
