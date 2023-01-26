'''RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
'''

__all__ = ['HeavyDBGeoLineStringType', 'GeoLineString']

from .metatype import HeavyDBMetaType
from .abstract_type import HeavyDBAbstractType
from rbc import typesystem
from numba.core import types as nb_types
from numba.core import extending


class GeoLineStringNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='GeoLineStringNumbaType')


class HeavyDBGeoLineStringType(HeavyDBAbstractType):
    """Typesystem type class for HeavyDB buffer structures.
    """

    def postprocess_type(self):
        return self.params(shorttypename='GeoLineString')

    def tonumba(self, bool_is_int8=None):
        flatbuffer_t = typesystem.Type.fromstring('int8_t* flatbuffer_')
        index_t = typesystem.Type.fromstring('int64_t* index_')
        n_t = typesystem.Type.fromstring('size_t n_')
        geoline_type = typesystem.Type(
            flatbuffer_t,
            index_t,
            n_t,
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


@extending.type_callable(GeoLineString)
def type_heavydb_timestamp(context):
    def typer():
        return typesystem.Type.fromstring('GeoLineString').tonumba()
    return typer


@extending.lower_builtin(GeoLineString)
def heavydb_timestamp_int_ctor(context, builder, sig, args):
    fa = context.make_helper(builder, sig.return_type)
    size_t = fa.n_.type
    fa.n_ = size_t(0)
    return fa._getvalue()
