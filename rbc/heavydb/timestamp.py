'''HeavyDB Timestamp type that corresponds to HeavyDB type TEXT ENCODED DICT.
'''
from rbc import typesystem
from rbc.heavydb import HeavyDBMetaType
from numba.core import extending, types
from llvmlite import ir

__all__ = ['HeavyDBTimestampType', 'Timestamp']


class Timestamp(object, metaclass=HeavyDBMetaType):
    pass


class HeavyDBTimestampType(typesystem.Type):
    """Typesystem type class for HeavyDB buffer structures.
    """
    @property
    def __typesystem_type__(self):
        return typesystem.Type('int64')

    def tostring(self, use_typename=False, use_annotation=True, use_name=True,
                 use_annotation_name=False, _skip_annotation=False):
        return 'Timestamp'


@extending.type_callable(Timestamp)
def type_heavydb_timestamp(context):
    def typer(arg):
        if isinstance(arg, types.Integer):
            return typesystem.Type.fromobject('Timestamp').tonumba()
    return typer


@extending.lower_builtin(Timestamp, types.Integer)
def heavydb_timestamp_constructor(context, builder, sig, args):
    return args[0]

    # int64_t = ir.IntType(64)
    # int8_t_ptr = ir.IntType(8).as_pointer()
    #
    # literal_value = sig.args[0].literal_value
    # sz = int64_t(len(literal_value))
    #
    # # arr = {ptr, size, is_null}*
    # arr = heavydb_buffer_constructor(context, builder, sig.return_type(types.int64), [sz])
    #
    # msg_bytes = literal_value.encode('utf-8')
    # msg_const = cgutils.make_bytearray(msg_bytes + b'\0')
    # try:
    #     msg_global_var = builder.module.get_global(literal_value)
    # except KeyError:
    #     msg_global_var = cgutils.global_constant(builder.module, literal_value, msg_const)
    # src_data = builder.bitcast(msg_global_var, int8_t_ptr)
    #
    # fn = get_copy_bytes_fn(builder.module, arr, src_data, sz)
    # builder.call(fn, [arr, src_data, sz])
    # return arr

