"""Implement Omnisci Column type support

Omnisci Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['OutputColumn', 'Column', 'OmnisciOutputColumnType', 'OmnisciColumnType',
           'OmnisciCursorType']

from llvmlite import ir
from rbc import typesystem
from .omnisci_buffer import Buffer, OmnisciBufferType, BufferType
from .column_list import OmnisciColumnListType
from rbc.targetinfo import TargetInfo
from numba.core import extending, types


int32_t = ir.IntType(32)


class OmnisciColumnType(OmnisciBufferType):
    """Omnisci Column type for RBC typesystem.
    """
    pass_by_value = True

    @property
    def buffer_extra_members(self):
        return (('int32_t dict_id', (5, 7)),)


class OmnisciOutputColumnType(OmnisciColumnType):
    """Omnisci OutputColumn type for RBC typesystem.

    OutputColumn is the same as Column but introduced to distinguish
    the input and output arguments of UDTFs.
    """


class Column(Buffer):
    pass


class OutputColumn(Column):
    pass


@extending.intrinsic
def omnisci_column_set_null_(typingctx, col_var, row_idx):
    # Float values are serialized as integers by OmniSciDB
    # For reference, here is the conversion table for float and double
    #   FLOAT:  1.1754944e-38            -> 8388608
    #   DOUBLE: 2.2250738585072014e-308  -> 4503599627370496
    #                    ^                          ^
    #                 fp value                  serialized
    T = col_var.eltype
    sig = types.void(col_var, row_idx)

    target_info = TargetInfo()
    null_value = target_info.null_values[str(T)]

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index = args

        assert data.opname == 'load'
        buf = data.operands[0]

        ptr = builder.load(builder.gep(buf, [zero, zero]))

        ty = ptr.type.pointee
        nv = ir.Constant(ir.IntType(T.bitwidth), null_value)
        if isinstance(T, types.Float):
            nv = builder.bitcast(nv, ty)
        builder.store(nv, builder.gep(ptr, [index]))

    return sig, codegen


@extending.overload_method(BufferType, 'set_null')
def omnisci_column_set_null(col_var, index):
    def impl(col_var, index):
        return omnisci_column_set_null_(col_var, index)
    return impl


@extending.intrinsic
def omnisci_column_is_null_(typingctx, col_var, row_idx):
    T = col_var.eltype
    sig = types.boolean(col_var, row_idx)

    target_info = TargetInfo()
    null_value = target_info.null_values[str(T)]
    nv = ir.Constant(ir.IntType(T.bitwidth), null_value)

    def codegen(context, builder, signature, args):
        zero = int32_t(0)
        data, index = args
        assert data.opname == 'load'
        buf = data.operands[0]

        ptr = builder.load(builder.gep(buf, [zero, zero]))
        res = builder.load(builder.gep(ptr, [index]))

        if isinstance(T, types.Float):
            res = builder.bitcast(res, nv.type)

        return builder.icmp_signed('==', res, nv)

    return sig, codegen


@extending.overload_method(BufferType, 'is_null')
def omnisci_column_is_null(col_var, row_idx):
    def impl(col_var, row_idx):
        return omnisci_column_is_null_(col_var, row_idx)
    return impl


@extending.intrinsic
def omnisci_column_get_dict_id_(typingctx, data):
    sig = types.int32(data)

    def codegen(context, builder, signature, args):
        data, = args
        col_typ = signature.args[0]
        column = context.make_helper(builder, col_typ, value=data)
        return column.dict_id
    return sig, codegen


@extending.overload_method(BufferType, 'get_dict_id')
def omnisci_column_get_dict_id(col_var):
    def impl(col_var):
        return omnisci_column_get_dict_id_(col_var)
    return impl


@extending.intrinsic
def omnisci_column_set_dict_id_(typingctx, data, dict_id):
    sig = types.void(data, dict_id)

    def codegen(context, builder, signature, args):
        zero, two = int32_t(0), int32_t(2)
        data, dict_id = args
        assert data.opname == 'load'
        buf = data.operands[0]
        builder.store(dict_id, builder.gep(buf, [zero, two]))

    return sig, codegen


@extending.overload_method(BufferType, 'set_dict_id')
def omnisci_column_set_dict_id(col_var, dict_id):
    def impl(col_var, dict_id):
        return omnisci_column_set_dict_id_(col_var, dict_id)
    return impl


@extending.intrinsic
def omnisci_column_is_dict_encoded_(typingctx, data):
    sig = types.boolean(data)

    def codegen(context, builder, signature, args):
        data, = args
        col_typ = signature.args[0]
        column = context.make_helper(builder, col_typ, value=data)
        return builder.icmp_signed('!=', column.dict_id, int32_t(-1))
    return sig, codegen


@extending.overload_method(BufferType, 'is_dict_encoded')
def omnisci_column_is_dict_encoded(col_var):
    def impl(col_var):
        return omnisci_column_is_dict_encoded_(col_var)
    return impl


class OmnisciCursorType(typesystem.Type):

    @classmethod
    def preprocess_args(cls, args):
        assert len(args) == 1
        params = []
        for p in args[0]:
            if not isinstance(p, (OmnisciColumnType, OmnisciColumnListType)):
                p = OmnisciColumnType((p,))
            params.append(p)
        return (tuple(params),)

    @property
    def as_consumed_args(self):
        return self[0]
