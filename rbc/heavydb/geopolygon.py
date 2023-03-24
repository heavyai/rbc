"""RBC GeoPolygon type that corresponds to HeavyDB type GeoPolygon.
"""

__all__ = ["HeavyDBGeoPolygonType", "GeoPolygon"]

from .geo_base import HeavyDBGeoBase, GeoBaseNumbaType, GeoBase


class GeoPolygonNumbaType(GeoBaseNumbaType):
    def __init__(self):
        super().__init__(name="GeoPolygonNumbaType")


class HeavyDBGeoPolygonType(HeavyDBGeoBase):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoPolygon"


class GeoPolygon(GeoBase):
    """
    RBC ``GeoPolygon`` type that corresponds to HeavyDB type GeoPolygon.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
