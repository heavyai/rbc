"""RBC GeoPolygon type that corresponds to HeavyDB type GeoPolygon.
"""

__all__ = ["HeavyDBGeoPolygonType", "GeoPolygon"]

from numba.core import extending

from rbc.external import external

from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray)
from .utils import as_voidptr, get_alloca


class GeoPolygonNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoPolygonType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoPolygonNumbaType

    @property
    def type_name(self):
        return "GeoPolygon"

    @property
    def item_type(self):
        return "GeoLineString"


class GeoPolygon(GeoNestedArray):
    """
    RBC ``GeoPolygon`` type that corresponds to HeavyDB type GeoPolygon.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """


@extending.overload_method(GeoPolygonNumbaType, "toCoords")
def heavydb_geopolygon_toCoords(geo):
    base_type = geo.base_type
    get_nrows = external(f"int64_t {base_type}_toCoords_get_nrows(int8_t*)|cpu")
    get_ncols = external(
        f"int64_t {base_type}_toCoords_get_ncols(int8_t*, int64_t)|cpu"
    )
    get_value = external(
        f"double {base_type}_toCoords_get_value(int8_t*, int64_t, int64_t)|cpu"
    )

    def impl(geo):
        lst = []
        geo_ptr = as_voidptr(get_alloca(geo))
        nrows = get_nrows(geo_ptr)
        for i in range(nrows):
            ncols = get_ncols(geo_ptr, i)
            inner = []
            for j in range(ncols):
                inner.append(get_value(geo_ptr, i, j))
            lst.append(inner)
        return lst

    return impl
