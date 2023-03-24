"""RBC GeoPolygon type that corresponds to HeavyDB type GeoPolygon.
"""

__all__ = ["HeavyDBGeoPolygonType", "GeoPolygon"]

from .geo_nested_array import HeavyDBGeoNestedArray, GeoNestedArrayNumbaType, GeoNestedArray


class GeoPolygonNumbaType(GeoNestedArrayNumbaType):
    def __init__(self):
        super().__init__(name="GeoPolygonNumbaType")


class HeavyDBGeoPolygonType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

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
