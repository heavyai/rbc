'''RBC GeoPoint type that corresponds to HeavyDB type GEOPOINT.
'''

__all__ = ['HeavyDBGeoPointType', 'GeoPoint']

from .metatype import HeavyDBMetaType
from .abstract_type import HeavyDBAbstractType
from numba.core import types as nb_types


class GeoPointNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='GeoPointNumbaType')


class HeavyDBGeoPointType(HeavyDBAbstractType):
    """Typesystem type class for HeavyDB buffer structures.
    """

    def postprocess_type(self):
        return self.params(shorttypename='GeoPoint')

    # A GeoPoint is not materialized, so, just skip the code below for now
    # as we can't test it.
    # def tonumba(self, bool_is_int8=None):
    #     ptr_t = typesystem.Type.fromstring('int8_t* ptr')
    #     sz_t = typesystem.Type.fromstring('int32_t sz')
    #     compression_t = typesystem.Type.fromstring('int32_t compression')
    #     input_srid_t = typesystem.Type.fromstring('int32_t input_srid')
    #     output_srid_t = typesystem.Type.fromstring('int32_t output_srid')
    #     struct = typesystem.Type(
    #         ptr_t,
    #         sz_t,
    #         compression_t,
    #         input_srid_t,
    #         output_srid_t,
    #     )
    #     geopoint_type = typesystem.Type(struct)
    #     return geopoint_type.tonumba(bool_is_int8=True)


class GeoPoint(object, metaclass=HeavyDBMetaType):
    """
    RBC ``GeoPoint`` type that corresponds to HeavyDB type GEOPOINT.

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
