"""RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
"""

__all__ = ["HeavyDBGeoLineStringType", "GeoLineString"]

from typing import List

from numba.core import extending
from numba.core import types as nb_types

from . import geopoint
from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec,
                               heavydb_geo_toCoords_vec)


class GeoLineStringNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoLineStringType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoLineStringNumbaType

    @property
    def type_name(self):
        return "GeoLineString"

    @property
    def item_type(self):
        return "Point2D"


class GeoLineString(GeoNestedArray):
    """
    RBC ``GeoLineString`` type that corresponds to HeavyDB type
    ``LINESTRING``.

    .. code-block:: c

        struct LineString {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
    def __getitem__(self, index: int) -> 'geopoint.GeoPoint':
        """
        Return the ``POINT`` at given index
        """

    def get_item(self, index: int) -> 'geopoint.GeoPoint':
        """
        Return the ``POINT`` at given index
        """

    def to_coords(self) -> List[float]:
        """
        .. note::
            Only available on ``CPU``
        """

    def from_coords(self, coords: List[float]) -> None:
        """
        .. note::
            Only available on ``CPU``
        """


@extending.overload_method(GeoLineStringNumbaType, "to_coords")
def heavydb_geolinestring_toCoords(geo):
    return heavydb_geo_toCoords_vec(geo)


@extending.overload_method(GeoLineStringNumbaType, "from_coords")
def heavydb_geolinestring_fromCoords(geo, lst):
    if isinstance(geo, GeoLineStringNumbaType) and isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec(geo, lst)
