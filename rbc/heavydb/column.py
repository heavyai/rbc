"""Implement Heavydb Column type support

Heavydb Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['OutputColumn', 'Column', 'HeavyDBOutputColumnType', 'HeavyDBColumnType',
           'HeavyDBCursorType']

from llvmlite import ir
from rbc import typesystem, irutils
from rbc.errors import NumbaTypeError
from .buffer import (Buffer, HeavyDBBufferType,
                     BufferType, BufferPointer,
                     heavydb_buffer_constructor)
from .column_list import HeavyDBColumnListType
from rbc.targetinfo import TargetInfo
from numba.core import extending, cgutils
from numba.core import types as nb_types
from typing import Union


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)


class HeavyDBColumnType(HeavyDBBufferType):
    """Heavydb Column type for RBC typesystem.
    """
    @property
    def pass_by_value(self):
        heavydb_version = TargetInfo().software[1][:3]
        return heavydb_version <= (5, 7, 0)

    def match(self, other):
        if type(self) is type(other):
            return self[0] == other[0]

    @property
    def buffer_extra_members(self):
        if self.element_type.tostring() == 'TextEncodingDict':
            return ('i8* string_dict_proxy_',)
        return ()


class HeavyDBOutputColumnType(HeavyDBColumnType):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn is the same as Column but introduced to distinguish
    the input and output arguments of UDTFs.
    """


class Column(Buffer):
    """
    RBC ``Column<T>`` type that corresponds to HeavyDB COLUMN

    In HeavyDB, a Column of type ``T`` is represented as follows:

    .. code-block:: c

        {
            T* ptr;
            int64_t sz;
            void* string_dict_proxy;  // available only if T == TextEncodingDict
        }

    """
    def dtype(self):
        """
        Data type of the array elements.
        """

    def getStringId(self, s: Union[str, 'TextEncodingNone']) -> int:  # noqa: F821
        """
        Return the string ID for the given string ``s``.

        .. note::
            Only available on ``CPU`` and ``Column<TextEncodingDict>``
        """

    def getString(self, id: int) -> int:
        """
        Return the string for the given ``id``.

        .. note::
            Only available on ``CPU`` and ``Column<TextEncodingDict>``
        """

    def __len__(self) -> int:
        """
        """


class OutputColumn(Column):
    pass


@extending.intrinsic
def heavydb_column_set_null_(typingctx, col_var, row_idx):
    # Float values are serialized as integers by HeavyDB
    # For reference, here is the conversion table for float and double
    #   FLOAT:  1.1754944e-38            -> 8388608
    #   DOUBLE: 2.2250738585072014e-308  -> 4503599627370496
    #                    ^                          ^
    #                 fp value                  serialized
    T = col_var.eltype
    sig = nb_types.void(col_var, row_idx)

    target_info = TargetInfo()
    null_value = target_info.null_values[str(T)]

    def codegen(context, builder, signature, args):
        data, index = args
        ptr = irutils.get_member_value(builder, data, 0)

        ty = ptr.type.pointee
        nv = ir.Constant(ir.IntType(T.bitwidth), null_value)
        if isinstance(T, nb_types.Float):
            nv = builder.bitcast(nv, ty)
        builder.store(nv, builder.gep(ptr, [index]))

    return sig, codegen


@extending.overload_method(BufferType, 'set_null')
def heavydb_column_set_null(col_var, index):
    def impl(col_var, index):
        return heavydb_column_set_null_(col_var, index)
    return impl


@extending.intrinsic
def heavydb_column_is_null_(typingctx, col_var, row_idx):
    T = col_var.eltype
    sig = nb_types.boolean(col_var, row_idx)

    target_info = TargetInfo()
    null_value = target_info.null_values[str(T)]
    nv = ir.Constant(ir.IntType(T.bitwidth), null_value)

    def codegen(context, builder, signature, args):
        data, index = args
        ptr = irutils.get_member_value(builder, data, 0)
        res = builder.load(builder.gep(ptr, [index]))

        if isinstance(T, nb_types.Float):
            res = builder.bitcast(res, nv.type)

        return builder.icmp_signed('==', res, nv)

    return sig, codegen


@extending.intrinsic
def heavydb_column_getString_(typingctx, col_var, string_id):
    def getBytes(builder, proxy, string_id):
        # bytes
        i8p = int8_t.as_pointer()
        fnty = ir.FunctionType(i8p, [i8p, int32_t])
        getStringBytes = cgutils.get_or_insert_function(builder.module, fnty,
                                                        "StringDictionaryProxy_getStringBytes")
        return builder.call(getStringBytes, [proxy, string_id])

    def getBytesLength(context, builder, proxy, string_id):
        # length
        i8p = int8_t.as_pointer()
        size_t = context.get_value_type(typesystem.Type('size_t')._normalize().tonumba())
        fnty = ir.FunctionType(size_t, [i8p, int32_t])
        getStringLength = cgutils.get_or_insert_function(builder.module, fnty,
                                                         "StringDictionaryProxy_getStringLength")
        return builder.call(getStringLength, [proxy, string_id])

    def free(builder, ptr):
        i8p = int8_t.as_pointer()
        void = ir.VoidType()
        fnty = ir.FunctionType(void, [i8p])
        fn = cgutils.get_or_insert_function(builder.module, fnty, "free")
        builder.call(fn, [ptr])

    def codegen(context, builder, signature, args):
        [col, string_id] = args
        string_id = builder.trunc(string_id, int32_t)
        proxy = builder.extract_value(builder.load(col), 2)

        ptr = getBytes(builder, proxy, string_id)
        sz = getBytesLength(context, builder, proxy, string_id)

        text = heavydb_buffer_constructor(context, builder, signature, [sz])
        cgutils.memcpy(builder, text.ptr, ptr, builder.add(sz, sz.type(1)))
        free(builder, ptr)
        text.sz = sz
        return text._getpointer()

    # importing it here to avoid circular import issue
    from .text_encoding_none import HeavyDBTextEncodingNoneType
    ret = HeavyDBTextEncodingNoneType().tonumba()
    sig = ret(col_var, string_id)
    return sig, codegen


@extending.intrinsic
def heavydb_column_getStringId_(typingctx, col_var, str_arg):
    # import here to avoid circular import issue
    from .text_encoding_none import TextEncodingNonePointer

    sig = nb_types.int32(col_var, str_arg)

    def codegen(context, builder, signature, args):
        [col, arg] = args
        if isinstance(str_arg, nb_types.UnicodeType):
            uni_str_ctor = cgutils.create_struct_proxy(nb_types.unicode_type)
            uni_str = uni_str_ctor(context, builder, value=arg)
            c_str = uni_str.data
        elif isinstance(str_arg, TextEncodingNonePointer):
            c_str = builder.extract_value(builder.load(arg), 0)
        else:
            raise NumbaTypeError(f'Cannot handle string argument type {str_arg}')
        proxy = builder.extract_value(builder.load(col), 2)
        i8p = int8_t.as_pointer()
        fnty = ir.FunctionType(int32_t, [i8p, i8p])
        fn = cgutils.get_or_insert_function(builder.module, fnty,
                                            "StringDictionaryProxy_getStringId")
        ret = builder.call(fn, [proxy, c_str])
        return ret

    return sig, codegen


@extending.overload_method(BufferType, 'is_null')
def heavydb_column_is_null(col_var, row_idx):
    def impl(col_var, row_idx):
        return heavydb_column_is_null_(col_var, row_idx)
    return impl


@extending.overload_method(BufferPointer, 'getString')
def heavydb_column_getString(col_var, row_idx):
    def impl(col_var, row_idx):
        return heavydb_column_getString_(col_var, row_idx)
    return impl


@extending.overload_method(BufferPointer, 'getStringId')
def heavydb_column_getStringId(col_var, str):
    def impl(col_var, str):
        return heavydb_column_getStringId_(col_var, str)
    return impl


class HeavyDBCursorType(typesystem.Type):

    @classmethod
    def preprocess_args(cls, args):
        assert len(args) == 1
        params = []
        for p in args[0]:
            if not isinstance(p, (HeavyDBColumnType, HeavyDBColumnListType)):
                # map Cursor<T ...> to Cursor<Column<T> ...>
                c = p.copy()
                p = HeavyDBColumnType((c,), **c._params)
                c._params.clear()
            params.append(p)
        return (tuple(params),)

    @property
    def as_consumed_args(self):
        return self[0]
