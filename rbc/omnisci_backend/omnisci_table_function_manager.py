__all__ = ['OmnisciTableFunctionManagerType']


from numba.core import extending, types
from numba.core.cgutils import make_bytearray, global_constant
from rbc import structure_type, irutils
from rbc.errors import UnsupportedError
from rbc.targetinfo import TargetInfo
from rbc.typesystem import Type
from llvmlite import ir


class OmnisciTableFunctionManagerType(Type):
    """TableFunctionManager<> is a typesystem custom type that
    represents a class type with the following public interface:

      class TableFunctionManager
      {
        int32_t error_message(const char* message);
        void set_output_row_size(int64_t num_rows);
      }

    """

    @property
    def __typesystem_type__(self):
        ptr_t = Type.fromstring("int8 ptr")
        return Type(ptr_t).params(NumbaPointerType=OmnisciTableFunctionManagerNumbaType).pointer()


class OmnisciTableFunctionManagerNumbaType(structure_type.StructureNumbaPointerType):
    pass


error_msg = 'TableFunctionManager is only available in OmniSciDB 5.8 or newer'


@extending.intrinsic
def omnisci_udtfmanager_error_message_(typingctx, mgr, msg):
    sig = types.int32(mgr, msg)

    if not isinstance(msg, types.StringLiteral):
        raise TypeError(f"expected StringLiteral but got {type(msg).__name__}")

    def codegen(context, builder, signature, args):
        mgr_ptr = args[0]

        mgr_i8ptr = builder.bitcast(mgr_ptr, ir.IntType(8).as_pointer())

        msg_bytes = msg.literal_value.encode('utf-8')
        msg_const = make_bytearray(msg_bytes + b'\0')
        msg_global_var = global_constant(
            builder.module,
            "table_function_manager_error_message",
            msg_const)
        msg_ptr = builder.bitcast(msg_global_var, ir.IntType(8).as_pointer())

        fnty = ir.FunctionType(
            ir.IntType(32), [
                ir.IntType(8).as_pointer(), ir.IntType(8).as_pointer()])
        fn = irutils.get_or_insert_function(
            builder.module, fnty, "TableFunctionManager_error_message")

        return builder.call(fn, [mgr_i8ptr, msg_ptr])

    return sig, codegen


@extending.overload_method(OmnisciTableFunctionManagerNumbaType, 'error_message')
def omnisci_udtfmanager_error_message(mgr, msg):
    def impl(mgr, msg):
        return omnisci_udtfmanager_error_message_(mgr, msg)
    return impl


@extending.intrinsic
def omnisci_udtfmanager_set_output_row_size_(typingctx, mgr, num_rows):
    sig = types.void(mgr, num_rows)

    target_info = TargetInfo()
    if target_info.software[1][:3] < (5, 8, 0):
        raise UnsupportedError(error_msg)

    def codegen(context, builder, sig, args):
        mgr_ptr, num_rows_arg = args

        mgr_i8ptr = builder.bitcast(mgr_ptr, ir.IntType(8).as_pointer())

        fnty = ir.FunctionType(ir.VoidType(), [ir.IntType(8).as_pointer(), ir.IntType(64)])
        fn = irutils.get_or_insert_function(
            builder.module, fnty, "TableFunctionManager_set_output_row_size")

        builder.call(fn, [mgr_i8ptr, num_rows_arg])

    return sig, codegen


@extending.overload_method(OmnisciTableFunctionManagerNumbaType, 'set_output_row_size')
def omnisci_udtfmanager_set_output_row_size(mgr, num_rows):
    def impl(mgr, num_rows):
        return omnisci_udtfmanager_set_output_row_size_(mgr, num_rows)
    return impl
