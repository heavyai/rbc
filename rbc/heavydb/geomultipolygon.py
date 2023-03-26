"""RBC GeoMultiPolygon type that corresponds to HeavyDB type GeoMultiPolygon.
"""

__all__ = ["HeavyDBGeoMultiPolygonType", "GeoMultiPolygon"]

from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray)


class GeoMultiPolygonNumbaType(GeoNestedArrayNumbaType):
    def __init__(self):
        super().__init__(name="GeoMultiPolygonNumbaType")


class HeavyDBGeoMultiPolygonType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

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
