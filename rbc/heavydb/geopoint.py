'''RBC GeoPoint type that corresponds to HeavyDB type GEOPOINT.
'''

__all__ = ['HeavyDBGeoPointType', 'GeoPoint']

from rbc import typesystem
from .metatype import HeavyDBMetaType
from numba.core import types as nb_types


class GeoPointNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='GeoPointNumbaType')


class HeavyDBGeoPointType(typesystem.Type):
    """Typesystem type class for HeavyDB buffer structures.
    """

    # A GeoPoint is not materialized
    def postprocess_type(self):
        return self.params(shorttypename='GeoPoint')



class GeoPoint(object, metaclass=HeavyDBMetaType):
    """
    RBC ``GeoPoint`` type that corresponds to HeavyDB type GEOPOINT.
    """
    pass
