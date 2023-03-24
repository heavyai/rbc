"""
Base classes for GEO types
"""

__all__ = ["GeoBaseNumbaType", "HeavyDBGeoBase", "GeoBase"]

from numba.core import types as nb_types

from rbc import typesystem

from .metatype import HeavyDBMetaType


class GeoBaseNumbaType(nb_types.Type):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBGeoBase(typesystem.Type):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def type_name(self):
        raise NotImplementedError()

    def postprocess_type(self):
        return self.params(shorttypename=self.type_name)

    def tonumba(self, bool_is_int8=None):
        flatbuffer_t = typesystem.Type.fromstring("int8_t* flatbuffer_")
        index_t = typesystem.Type.fromstring("int64_t index_t")
        n_t = typesystem.Type.fromstring("int64_t n_")
        geoline_type = typesystem.Type(
            flatbuffer_t,
            # int64_t index[4]
            index_t,
            index_t,
            index_t,
            index_t,
            n_t,
            name=self.type_name,
        )
        return geoline_type.tonumba(bool_is_int8=True)


class GeoBase(metaclass=HeavyDBMetaType):
    """
    Base class for GEO types.

    .. note::
        Geo columns should inherit from ``ColumnFlatBuffer`` in
        ``column_flatbuffer.py``.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """

    pass
