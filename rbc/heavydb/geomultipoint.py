"""RBC GeoMultiPoint type that corresponds to HeavyDB type GeoMultiPoint.
"""

__all__ = ["HeavyDBGeoMultiPointType", "GeoMultiPoint"]

from typing import List

from numba.core import extending
from numba.core import types as nb_types

from . import geopoint
from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray,
                               heavydb_geo_fromCoords_vec,
                               heavydb_geo_toCoords_vec)


class GeoMultiPointNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoMultiPointType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoMultiPointNumbaType

    @property
    def type_name(self):
        return "GeoMultiPoint"

    @property
    def item_type(self):
        return "Point2D"


class GeoMultiPoint(GeoNestedArray):
    """
    RBC ``GeoMultiPoint`` type that corresponds to HeavyDB type
    ``MULTIPOINT``.

    .. code-block:: c

        Struct MultiPoint {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """

    def __getitem__(self, index: int) -> 'geopoint.GeoPoint':
        """
        Return the ``POINT`` at the given index
        """

    def get_item(self, index: int) -> 'geopoint.GeoPoint':
        """
        Return the ``POINT`` at the given index
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


@extending.overload_method(GeoMultiPointNumbaType, "to_coords")
def heavydb_geomultipoint_toCoords(geo):
    return heavydb_geo_toCoords_vec(geo)


@extending.overload_method(GeoMultiPointNumbaType, "from_coords")
def heavydb_geomultipoint_fromCoords(geo, lst):
    if isinstance(geo, GeoMultiPointNumbaType) and isinstance(lst, nb_types.List):
        return heavydb_geo_fromCoords_vec(geo, lst)


@extending.overload_method(GeoMultiPointNumbaType, "n_coords")
def heavydb_geomultipoint_ncoords(geo):
    # For GeoLineString and GeoMultiPoint, the number of coordinates is
    # 2 * number of points
    def impl(geo):
        return len(geo) * 2
    return impl
