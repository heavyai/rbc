'''RBC GeoLineString type that corresponds to HeavyDB type GEOLINESTRING.
'''

__all__ = ['HeavyDBGeoLineStringType', 'GeoLineString']

from numba.core import extending
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
        col_ptr_t = typesystem.Type.fromstring('int8_t* column_ptr_')
        index_t = typesystem.Type.fromstring('int64_t index_')
        geoline_type = typesystem.Type(
            col_ptr_t,
            index_t,
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


@extending.type_callable(GeoLineString)
def type_heavydb_timestamp(context):
    def typer(ptr, idx):
        if isinstance(ptr, nb_types.CPointer) and isinstance(idx, nb_types.Integer):
            return typesystem.Type.fromstring('GeoLineString').tonumba()
    return typer


@extending.lower_builtin(GeoLineString, nb_types.CPointer, nb_types.Integer)
def heavydb_timestamp_int_ctor(context, builder, sig, args):
    fa = context.make_helper(builder, sig.return_type)
    fa.column_ptr_ = args[0]
    fa.index_ = args[1]
    return fa._getvalue()
