"""RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
"""

__all__ = ["HeavyDBGeoLineStringType", "GeoLineString"]

from .geo_nested_array import HeavyDBGeoNestedArray, GeoNestedArrayNumbaType, GeoNestedArray


class GeoLineStringNumbaType(GeoNestedArrayNumbaType):
    def __init__(self):
        super().__init__(name="GeoLineStringNumbaType")


class HeavyDBGeoLineStringType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoLineString"

    @property
    def item_type(self):
        return "Point2D"


class GeoLineString(GeoNestedArray):
    """
    RBC ``GeoLineString`` type that corresponds to HeavyDB type GEOLINESTRING.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
