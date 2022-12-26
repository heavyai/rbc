'''RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
'''

__all__ = ['HeavyDBGeoLineStringType', 'GeoLineString']

from .metatype import HeavyDBMetaType
from .abstract_type import HeavyDBAbstractType
from rbc import typesystem
from numba.core import types as nb_types


class GeoLineStringNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='GeoLineStringNumbaType')


class HeavyDBGeoLineStringType(HeavyDBAbstractType):
    """Typesystem type class for HeavyDB buffer structures.
    """

    def postprocess_type(self):
        return self.params(shorttypename='GeoLineString')

    def tonumba(self, bool_is_int8=None):
        ptr_t = typesystem.Type.fromstring('int8_t* ptr')
        sz_t = typesystem.Type.fromstring('int32_t sz')
        compression_t = typesystem.Type.fromstring('int32_t compression')
        input_srid_t = typesystem.Type.fromstring('int32_t input_srid')
        output_srid_t = typesystem.Type.fromstring('int32_t output_srid')
        geoline_type = typesystem.Type(
            ptr_t,
            sz_t,
            compression_t,
            input_srid_t,
            output_srid_t,
            name='GeoLineString')
        return geoline_type.tonumba(bool_is_int8=True)


class GeoLineString(object, metaclass=HeavyDBMetaType):
    """
    RBC ``GeoLineString`` type that corresponds to HeavyDB type GEOLINESTRING.

    .. code-block:: c

        {
            int8_t* ptr;
            int32_t sz;
            int32_t compression;
            int32_t input_srid;
            int32_t output_srid;
        }
    """
    pass
