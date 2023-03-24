"""RBC GeoMultiLineString type that corresponds to HeavyDB type GeoMultiLineString.
"""

__all__ = ["HeavyDBGeoMultiLineStringType", "GeoMultiLineString"]

from .geo_base import HeavyDBGeoBase, GeoBaseNumbaType, GeoBase


class GeoMultiLineStringNumbaType(GeoBaseNumbaType):
    def __init__(self):
        super().__init__(name="GeoMultiLineStringNumbaType")


class HeavyDBGeoMultiLineStringType(HeavyDBGeoBase):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoMultiLineString"


class GeoMultiLineString(GeoBase):
    """
    RBC ``GeoMultiLineString`` type that corresponds to HeavyDB type GeoMultiLineString.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
