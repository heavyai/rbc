"""RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
"""

__all__ = ["HeavyDBGeoLineStringType", "GeoLineString"]

from .geo_base import HeavyDBGeoBase, GeoBaseNumbaType, GeoBase


class GeoLineStringNumbaType(GeoBaseNumbaType):
    def __init__(self):
        super().__init__(name="GeoLineStringNumbaType")


class HeavyDBGeoLineStringType(HeavyDBGeoBase):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoLineString"


class GeoLineString(GeoBase):
    """
    RBC ``GeoLineString`` type that corresponds to HeavyDB type GEOLINESTRING.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
