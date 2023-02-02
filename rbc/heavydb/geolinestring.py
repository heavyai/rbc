'''RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
'''

__all__ = ['HeavyDBGeoLineStringType', 'GeoLineString']

from numba.core import types as nb_types

from rbc import typesystem

from .metatype import HeavyDBMetaType


class GeoLineStringNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='GeoLineStringNumbaType')


class HeavyDBGeoLineStringType(typesystem.Type):
    """Typesystem type class for HeavyDB buffer structures.
    """

    def postprocess_type(self):
        return self.params(shorttypename='GeoLineString')

    def tonumba(self, bool_is_int8=None):
        flatbuffer_t = typesystem.Type.fromstring('int8_t* flatbuffer_')
        index_t = typesystem.Type.fromstring('int64_t index_t')
        n_t = typesystem.Type.fromstring('int64_t n_')
        geoline_type = typesystem.Type(
            flatbuffer_t,
            # int64_t index[4]
            index_t,
            index_t,
            index_t,
            index_t,
            n_t,
            name='GeoLineString')
        return geoline_type.tonumba(bool_is_int8=True)


class GeoLineString(object, metaclass=HeavyDBMetaType):
    """
    RBC ``GeoLineString`` type that corresponds to HeavyDB type GEOLINESTRING.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
    pass
