"""HeavyDB Timestamp type that corresponds to HeavyDB type timestamp."""
from rbc import typesystem
from rbc.heavydb import HeavyDBMetaType
from numba.core import extending, types, cgutils

__all__ = ['HeavyDBTimestampType', 'Timestamp']


class Timestamp(object, metaclass=HeavyDBMetaType):
    pass


class TimestampNumbaType(types.Type):
    pass


class HeavyDBTimestampType(typesystem.Type):
    """Typesystem type class for HeavyDB timestamp structures.
    """
    @property
    def __typesystem_type__(self):
        return typesystem.Type.fromstring('{int64 time}').params(
            name='Timestamp', NumbaType=TimestampNumbaType)

    def tonumba(self, bool_is_int8=None):
        timestamp = self.__typesystem_type__
        numba_type = timestamp.tonumba()
        return numba_type

    def tostring(self, use_typename=False, use_annotation=True, use_name=True,
                 use_annotation_name=False, _skip_annotation=False):
        return 'Timestamp'


extending.make_attribute_wrapper(TimestampNumbaType, 'time', 'time')


@extending.type_callable(Timestamp)
def type_heavydb_timestamp(context):
    def typer(arg):
        if isinstance(arg, types.Integer):
            return typesystem.Type.fromobject('Timestamp').tonumba()
    return typer


@extending.lower_builtin(Timestamp, types.Integer)
def heavydb_timestamp_constructor(context, builder, sig, args):
    time = args[0]
    typ = sig.return_type
    timestamp = cgutils.create_struct_proxy(typ)(context, builder)
    timestamp.time = time
    return timestamp._getvalue()
