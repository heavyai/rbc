__all__ = ['HeavyDBRowFunctionManagerType', 'RowFunctionManager']


from numba.core import extending, cgutils
from numba.core.cgutils import make_bytearray, global_constant
from numba.core import types as nb_types
from rbc import structure_type
from rbc.errors import UnsupportedError, RequireLiteralValue
from rbc.targetinfo import TargetInfo
from rbc.typesystem import Type
from .abstract_type import HeavyDBAbstractType
from .metatype import HeavyDBMetaType
from . import text_encoding_none, string_dict_proxy
from llvmlite import ir


class HeavyDBRowFunctionManagerType(HeavyDBAbstractType):
    """RowFunctionManager<> is a typesystem custom type that
    represents a class type with the following public interface:

      struct RowFunctionManager { }

    """

    @property
    def __typesystem_type__(self):
        ptr_t = Type.fromstring("int8 pointer")
        return Type(ptr_t).params(NumbaPointerType=HeavyDBRowFunctionManagerNumbaType).pointer()


class HeavyDBRowFunctionManagerNumbaType(structure_type.StructureNumbaPointerType):
    pass


class RowFunctionManager(metaclass=HeavyDBMetaType):
    """
    TableFunctionManager is available in HeavyDB 6.2 or newer
    """

    TRANSIENT_DICT_DB_ID = 0
    TRANSIENT_DICT_ID = 0

    def getString(self, db_id: int, dict_id: int, str_arg: int) -> 'text_encoding_none.TextEncodingNone':
        """
        """

    def getStringDictionaryProxy(self, db_id: int, dict_id: int) -> 'string_dict_proxy.StringDictionaryProxy':
        """
        """

    def getDictDbId(self, func_name: str, arg_idx: int) -> int:
        """
        """

    def getDictId(self, func_name: str, arg_idx: int) -> int:
        """
        """


error_msg = 'RowFunctionManager is only available in HeavyDB 6.2 or newer (got %s)'
i8p = ir.IntType(8).as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)


__DB_ID = 0x0f
__DICT_ID = 0x0e


@extending.intrinsic
def heavydb_udf_manager_get_dict_id_(typingctx, mgr, kind, func_name, arg_idx):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    if not isinstance(func_name, nb_types.StringLiteral):
        raise RequireLiteralValue(f"expected StringLiteral but got {type(func_name).__name__}")

    if not isinstance(kind, nb_types.IntegerLiteral):
        raise RequireLiteralValue(f"expected IntegerLiteral but got {kind}")

    sig = nb_types.int32(mgr, kind, func_name, arg_idx)

    suffix = 'getDictDbId' if kind.literal_value == __DB_ID else 'getDictId'

    def codegen(context, builder, signature, args):
        mgr_ptr, _, _, idx = args

        mgr_i8ptr = builder.bitcast(mgr_ptr, i8p)

        msg_bytes = func_name.literal_value.encode('utf-8')
        msg_const = make_bytearray(msg_bytes + b'\0')
        msg_global_var = global_constant(
            builder.module,
            "row_function_manager_func_name",
            msg_const)
        msg_ptr = builder.bitcast(msg_global_var, i8p)

        fnty = ir.FunctionType(i32, [i8p, i8p, i64])
        fn = cgutils.get_or_insert_function(
            builder.module, fnty, f"RowFunctionManager_{suffix}")

        return builder.call(fn, [mgr_i8ptr, msg_ptr, idx])

    return sig, codegen


@extending.intrinsic
def heavydb_udf_manager_get_string_dict_proxy_(typingctx, mgr, db_id, dict_id):
    from .string_dict_proxy import StringDictionaryProxyNumbaType
    dict_proxy = StringDictionaryProxyNumbaType()
    sig = dict_proxy(mgr, db_id, dict_id)

    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    def codegen(context, builder, signature, args):
        mgr_ptr, db_id, dict_id = args

        mgr_i8ptr = builder.bitcast(mgr_ptr, i8p)
        fnty = ir.FunctionType(i8p, [i8p, i32, i32])
        fn = cgutils.get_or_insert_function(
            builder.module, fnty, "RowFunctionManager_getStringDictionaryProxy")
        dict_id = builder.trunc(dict_id, i32)
        db_id = builder.trunc(db_id, i32)
        proxy_ptr = builder.call(fn, [mgr_i8ptr, db_id, dict_id])
        proxy_ctor = cgutils.create_struct_proxy(sig.return_type)
        proxy = proxy_ctor(context, builder)
        proxy.ptr = proxy_ptr
        return proxy._getvalue()

    return sig, codegen


@extending.overload_method(HeavyDBRowFunctionManagerNumbaType, 'getDictId')
def heavydb_udf_manager_get_dict_id(mgr, func_name, arg_idx):
    def impl(mgr, func_name, arg_idx):
        return heavydb_udf_manager_get_dict_id_(mgr, __DICT_ID, func_name, arg_idx)
    return impl


@extending.overload_method(HeavyDBRowFunctionManagerNumbaType, 'getDictDbId')
def heavydb_udf_manager_get_db_id(mgr, func_name, arg_idx):
    def impl(mgr, func_name, arg_idx):
        return heavydb_udf_manager_get_dict_id_(mgr, __DB_ID, func_name, arg_idx)
    return impl


@extending.overload_method(HeavyDBRowFunctionManagerNumbaType, 'getStringDictionaryProxy')
def heavydb_udf_manager_get_string_dict_proxy(mgr, db_id, dict_id):
    def impl(mgr, db_id, dict_id):
        return heavydb_udf_manager_get_string_dict_proxy_(mgr, db_id, dict_id)
    return impl


@extending.overload_method(HeavyDBRowFunctionManagerNumbaType, 'getOrAddTransient')
def heavydb_udf_manager_get_or_add_transient(mgr, db_id, dict_id, str_arg):
    def impl(mgr, db_id, dict_id, str_arg):
        proxy = mgr.getStringDictionaryProxy(db_id, dict_id)
        return proxy.getOrAddTransient(str_arg)
    return impl


@extending.overload_method(HeavyDBRowFunctionManagerNumbaType, 'getString')
def heavydb_udf_manager_get_string(mgr, db_id, dict_id, string_id):
    def impl(mgr, db_id, dict_id, string_id):
        proxy = mgr.getStringDictionaryProxy(db_id, dict_id)
        return proxy.getString(string_id)
    return impl


@extending.overload_attribute(HeavyDBRowFunctionManagerNumbaType, 'TRANSIENT_DICT_DB_ID')
@extending.overload_attribute(HeavyDBRowFunctionManagerNumbaType, 'TRANSIENT_DICT_ID')
def heavydb_udf_manager_transient_dict_id(mgr):
    def impl(mgr):
        return 0
    return impl
