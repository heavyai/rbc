"""RBC GeoPoint type that corresponds to HeavyDB type GEOPOINT.
"""

__all__ = ["HeavyDBGeoPointType", "GeoPoint"]

from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray)


class GeoPointNumbaType(GeoNestedArrayNumbaType):
    def __init__(self):
        super().__init__(name="GeoPointNumbaType")


class HeavyDBGeoPointType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        return "GeoPoint"


class GeoPoint(GeoNestedArray):
    """
    RBC ``GeoPoint`` type that corresponds to HeavyDB type GEOPOINT.
    """
