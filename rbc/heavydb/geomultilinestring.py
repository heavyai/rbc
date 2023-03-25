"""RBC GeoMultiLineString type that corresponds to HeavyDB type GeoMultiLineString.
"""

__all__ = ["HeavyDBGeoMultiLineStringType", "GeoMultiLineString"]

from .geo_nested_array import HeavyDBGeoNestedArray, GeoNestedArrayNumbaType, GeoNestedArray


class GeoMultiLineStringNumbaType(GeoNestedArrayNumbaType):
    def __init__(self):
        super().__init__(name="GeoMultiLineStringNumbaType")


class HeavyDBGeoMultiLineStringType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

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
