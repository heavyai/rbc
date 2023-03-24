"""RBC GeoPoint type that corresponds to HeavyDB type GEOPOINT.
"""

__all__ = ["HeavyDBGeoPointType", "GeoPoint"]

from .geo_base import HeavyDBGeoBase, GeoBaseNumbaType, GeoBase


class GeoPointNumbaType(GeoBaseNumbaType):
    def __init__(self):
        super().__init__(name="GeoPointNumbaType")


class HeavyDBGeoPointType(HeavyDBGeoBase):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoPoint"


class GeoPoint(GeoBase):
    """
    RBC ``GeoPoint`` type that corresponds to HeavyDB type GEOPOINT.
    """
