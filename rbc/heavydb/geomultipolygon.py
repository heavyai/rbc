"""RBC GeoMultiPolygon type that corresponds to HeavyDB type GeoMultiPolygon.
"""

__all__ = ["HeavyDBGeoMultiPolygonType", "GeoMultiPolygon"]

from .geo_base import HeavyDBGeoBase, GeoBaseNumbaType, GeoBase


class GeoMultiPolygonNumbaType(GeoBaseNumbaType):
    def __init__(self):
        super().__init__(name="GeoMultiPolygonNumbaType")


class HeavyDBGeoMultiPolygonType(HeavyDBGeoBase):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoMultiPolygon"


class GeoMultiPolygon(GeoBase):
    """
    RBC ``GeoMultiPolygon`` type that corresponds to HeavyDB type GeoMultiPolygon.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
