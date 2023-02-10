__all__ = ['HeavyDBRowFunctionManagerType', 'RowFunctionManager']


from numba.core import extending, typing
from numba.core import types as nb_types

from rbc.errors import UnsupportedError
from rbc.external import external
from rbc.targetinfo import TargetInfo

from . import string_dict_proxy, text_encoding_none
from .metatype import HeavyDBMetaType
from .opaque_pointer import HeavyDBOpaquePtr, OpaquePtrNumbaType
from .utils import as_voidptr, global_str_constant


class HeavyDBRowFunctionManagerType(HeavyDBOpaquePtr):
    """RowFunctionManager<> is a typesystem custom type that
    represents a class type with the following public interface:

      struct RowFunctionManager { }
    """

    @property
    def numba_type(self):
        return RowFunctionManagerNumbaType

    @property
    def type_name(self):
        return "RowFunctionManager"


class RowFunctionManagerNumbaType(OpaquePtrNumbaType):
    pass


class RowFunctionManager(metaclass=HeavyDBMetaType):
    """
    TableFunctionManager is available in HeavyDB 6.2 or newer
    """

    TRANSIENT_DICT_DB_ID = 0
    TRANSIENT_DICT_ID = 0

    def getString(self, db_id: int, dict_id: int, str_arg: int) -> 'text_encoding_none.TextEncodingNone':  # noqa: E501
        """
        """

    def getStringDictionaryProxy(self, db_id: int, dict_id: int) -> 'string_dict_proxy.StringDictionaryProxy':  # noqa: E501
        """
        """

    def getDictDbId(self, func_name: str, arg_idx: int) -> int:
        """
        """

    def getDictId(self, func_name: str, arg_idx: int) -> int:
        """
        """


error_msg = 'RowFunctionManager is only available in HeavyDB 6.2 or newer (got %s)'


@extending.overload_method(RowFunctionManagerNumbaType, 'getDictId')
def heavydb_udf_manager_get_dict_id(mgr, func_name, arg_idx):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    defn = 'int32 RowFunctionManager_getDictId(int8*, int8*, int32)'
    get_dict_id_ = external(defn)

    def impl(mgr, func_name, arg_idx):
        func_name_ = global_str_constant("row_mgr_func_name", func_name)
        return get_dict_id_(as_voidptr(mgr), func_name_, arg_idx)
    return impl


@extending.overload_method(RowFunctionManagerNumbaType, 'getDictDbId')
def heavydb_udf_manager_get_db_id(mgr, func_name, arg_idx):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    defn = 'int32 RowFunctionManager_getDictDbId(int8*, int8*, int32)|CPU'
    get_dict_db_id_ = external(defn)

    def impl(mgr, func_name, arg_idx):
        func_name_ = global_str_constant("row_mgr_func_name", func_name)
        return get_dict_db_id_(as_voidptr(mgr), func_name_, arg_idx)
    return impl


@extending.overload_method(RowFunctionManagerNumbaType, 'getStringDictionaryProxy')
def heavydb_udf_manager_get_string_dict_proxy(mgr, db_id, dict_id):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    i8p = nb_types.CPointer(nb_types.int8)
    i32 = nb_types.int32
    proxy = string_dict_proxy.StringDictionaryProxyNumbaType()
    sig = typing.signature(proxy, i8p, i32, i32)

    symbol = 'RowFunctionManager_getStringDictionaryProxy'
    get_string_dict_proxy_ = nb_types.ExternalFunction(symbol, sig)

    def impl(mgr, db_id, dict_id):
        return get_string_dict_proxy_(as_voidptr(mgr), db_id, dict_id)
    return impl


@extending.overload_method(RowFunctionManagerNumbaType, 'getOrAddTransient')
def heavydb_udf_manager_get_or_add_transient(mgr, db_id, dict_id, str_arg):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    def impl(mgr, db_id, dict_id, str_arg):
        proxy = mgr.getStringDictionaryProxy(db_id, dict_id)
        return proxy.getOrAddTransient(str_arg)
    return impl


@extending.overload_method(RowFunctionManagerNumbaType, 'getString')
def heavydb_udf_manager_get_string(mgr, db_id, dict_id, string_id):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    def impl(mgr, db_id, dict_id, string_id):
        proxy = mgr.getStringDictionaryProxy(db_id, dict_id)
        return proxy.getString(string_id)
    return impl


@extending.overload_attribute(RowFunctionManagerNumbaType, 'TRANSIENT_DICT_DB_ID')
@extending.overload_attribute(RowFunctionManagerNumbaType, 'TRANSIENT_DICT_ID')
def heavydb_udf_manager_transient_dict_id(mgr):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 2, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    def impl(mgr):
        return 0
    return impl
