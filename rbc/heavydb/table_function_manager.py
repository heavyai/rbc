__all__ = ['HeavyDBTableFunctionManagerType', 'TableFunctionManager']


from numba.core import extending, types

from rbc.errors import RequireLiteralValue, UnsupportedError
from rbc.external import external
from rbc.targetinfo import TargetInfo

from .metatype import HeavyDBMetaType
from .opaque_pointer import HeavyDBOpaquePtr, OpaquePtrNumbaType
from .utils import as_voidptr, global_str_constant


class HeavyDBTableFunctionManagerType(HeavyDBOpaquePtr):
    """TableFunctionManager<> is a typesystem custom type that
    represents a class type with the following public interface:

      class TableFunctionManager
      {
        int32_t error_message(const char* message);
        void set_output_row_size(int64_t num_rows);
      }

    """

    @property
    def numba_type(self):
        return TableFunctionManagerNumbaType

    @property
    def type_name(self):
        return 'TableFunctionManager'


class TableFunctionManagerNumbaType(OpaquePtrNumbaType):
    pass


class TableFunctionManager(metaclass=HeavyDBMetaType):
    """
    TableFunctionManager is available in HeavyDB 5.9 or newer
    """

    def set_output_row_size(self, size: int) -> None:
        """
        Set the number of rows in an output column.

        .. note::
            Must be called before any assignment on output columns.
        """

    def error_message(self, msg: str) -> None:
        """
        .. note::
            ``msg`` must be known at compile-time.
        """

    def set_output_array_values_total_number(self, index: int,
                                             output_array_values_total_number: int) -> None:
        """
        Set the total number of array values in a column of arrays.

        .. note::
            Must be called before making any assignment on output columns.
        """


error_msg = 'TableFunctionManager is only available in HeavyDB 5.9 or newer (got %s)'


@extending.overload_method(TableFunctionManagerNumbaType, 'error_message')
def heavydb_udtfmanager_error_message(mgr, msg):
    target_info = TargetInfo()
    if target_info.software[1][:3] < (5, 9, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    if not isinstance(msg, types.StringLiteral):
        raise RequireLiteralValue(f"expected StringLiteral but got {type(msg).__name__}")

    defn = 'int32_t TableFunctionManager_error_message(int8_t*, int8_t*)'
    mgr_error_message_ = external(defn, devices=['CPU'])

    identifier = "table_function_manager_error_message"

    def impl(mgr, msg):
        return mgr_error_message_(as_voidptr(mgr),
                                  global_str_constant(identifier, msg))
    return impl


@extending.overload_method(TableFunctionManagerNumbaType, 'set_output_row_size')
def heavydb_udtfmanager_set_output_row_size(mgr, num_rows):

    target_info = TargetInfo()
    if target_info.software[1][:3] < (5, 9, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    defn = 'void TableFunctionManager_set_output_row_size(int8_t*, int64_t)'
    mgr_set_output_row_size_ = external(defn, devices=['CPU'])

    def impl(mgr, num_rows):
        return mgr_set_output_row_size_(as_voidptr(mgr), num_rows)
    return impl


set_output_array = 'set_output_array_values_total_number'


@extending.overload_method(TableFunctionManagerNumbaType, set_output_array)
def mgr_set_output_array(mgr, col_idx, value):

    # XXX: Check is heavydb 6.1 has this function
    target_info = TargetInfo()
    if target_info.software[1][:3] < (6, 1, 0):
        raise UnsupportedError(error_msg % (".".join(map(str, target_info.software[1]))))

    defn = ('void TableFunctionManager_set_output_array_values_total_number'
            '(int8_t*, int32_t, int64_t)')
    mgr_set_output_array_ = external(defn, devices=['CPU'])

    def impl(mgr, col_idx, value):
        mgr_set_output_array_(as_voidptr(mgr), col_idx, value)
    return impl
