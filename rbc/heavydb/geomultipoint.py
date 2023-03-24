"""RBC GeoMultiPoint type that corresponds to HeavyDB type GeoMultiPoint.
"""

__all__ = ["HeavyDBGeoMultiPointType", "GeoMultiPoint"]

from .geo_base import HeavyDBGeoBase, GeoBaseNumbaType, GeoBase


class GeoMultiPointNumbaType(GeoBaseNumbaType):
    def __init__(self):
        super().__init__(name="GeoMultiPointNumbaType")


class HeavyDBGeoMultiPointType(HeavyDBGeoBase):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoMultiPoint"


class GeoMultiPoint(GeoBase):
    """
    RBC ``GeoMultiPoint`` type that corresponds to HeavyDB type GeoMultiPoint.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
