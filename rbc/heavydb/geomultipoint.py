"""RBC GeoMultiPoint type that corresponds to HeavyDB type GeoMultiPoint.
"""

__all__ = ["HeavyDBGeoMultiPointType", "GeoMultiPoint"]

from .geo_nested_array import HeavyDBGeoNestedArray, GeoNestedArrayNumbaType, GeoNestedArray


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
