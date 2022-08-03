"""
RBC TextEncodingNone type that corresponds to HeavyDB type TEXT ENCODED NONE.
"""

__all__ = ['TextEncodingNonePointer', 'TextEncodingNone', 'HeavyDBTextEncodingNoneType']

import operator
from rbc import typesystem
from rbc.targetinfo import TargetInfo
from rbc.errors import RequireLiteralValue
from .buffer import (
    Buffer, HeavyDBBufferType,
    heavydb_buffer_constructor)
from numba.core import extending, cgutils
from numba.core import types as nb_types
from .array import ArrayPointer
from llvmlite import ir
from typing import Union


int32_t = ir.IntType(32)
int64_t = ir.IntType(64)


class HeavyDBTextEncodingNoneType(HeavyDBBufferType):
    """HeavyDB TextEncodingNone type for RBC typesystem.
    """

    @property
    def numba_pointer_type(self):
        return TextEncodingNonePointer

    @classmethod
    def preprocess_args(cls, args):
        element_type = typesystem.Type.fromstring('char8')
        return ((element_type,),)

    @property
    def buffer_extra_members(self):
        return ('bool is_null',)

    def match(self, other):
        if type(self) is type(other):
            return self[0] == other[0]
        if other.is_pointer and other[0].is_char and other[0].bits == 8:
            return 1
        if other.is_string:
            return 2


class TextEncodingNonePointer(ArrayPointer):
    def deepcopy(self, context, builder, val, retptr):
        from .buffer import memalloc
        ptr_type = self.dtype.members[0]
        element_size = int64_t(ptr_type.dtype.bitwidth // 8)

        struct_load = builder.load(val)
        src = builder.extract_value(struct_load, 0, name='text_buff_ptr')
        element_count = builder.extract_value(struct_load, 1, name='text_size')
        is_null = builder.extract_value(struct_load, 2, name='text_is_null')

        zero, one, two = int32_t(0), int32_t(1), int32_t(2)
        ptr = memalloc(context, builder, ptr_type, element_count, element_size)
        cgutils.raw_memcpy(builder, ptr, src, element_count, element_size)
        builder.store(ptr, builder.gep(retptr, [zero, zero]))
        builder.store(element_count, builder.gep(retptr, [zero, one]))
        builder.store(is_null, builder.gep(retptr, [zero, two]))


class TextEncodingNone(Buffer):
    """
    RBC ``TextEncodingNone`` type that corresponds to HeavyDB type TEXT ENCODED NONE.
    HeavyDB TextEncodingNone represents the following structure:

    .. code-block:: c

        struct TextEncodingNone {
            char* ptr;
            size_t size;
            int8_t padding;
        }

    .. code-block:: python

        from rbc.heavydb import TextEncodingNone
        @heavydb('TextEncodingNone(int32, int32)')
        def make_abc(first, n):
            r = TextEncodingNone(n)
            for i in range(n):
                r[i] = first + i
            return r

    .. code-block:: python

        from rbc.heavydb import TextEncodingNone
        @heavydb('TextEncodingNone()')
        def make_text():
            return TextEncodingNone('some text here')

    """

    def __init__(self, size: Union[int, str]):
        pass


@extending.overload(operator.eq)
def text_encoding_none_eq(a, b):
    if isinstance(a, TextEncodingNonePointer) and isinstance(b, TextEncodingNonePointer):

        def impl(a, b):
            if len(a) != len(b):
                return False
            for i in range(0, len(a)):
                if a[i] != b[i]:
                    return False
            return True
        return impl
    elif isinstance(a, TextEncodingNonePointer) and isinstance(b, nb_types.StringLiteral):
        lv = b.literal_value
        sz = len(lv)

        def impl(a, b):
            if len(a) != sz:
                return False
            t = TextEncodingNone(lv)
            return a == t
        return impl


@extending.overload(operator.ne)
def text_encoding_none_ne(a, b):
    if isinstance(a, TextEncodingNonePointer):
        if isinstance(b, (TextEncodingNonePointer, nb_types.StringLiteral)):
            def impl(a, b):
                return not (a == b)
            return impl


@extending.lower_builtin(TextEncodingNone, nb_types.Integer)
def heavydb_text_encoding_none_constructor(context, builder, sig, args):
    return heavydb_buffer_constructor(context, builder, sig, args)._getpointer()


def get_copy_bytes_fn(module, arr, src_data, sz):

    fn_name = 'copy_bytes_fn'
    try:
        return module.get_global(fn_name)
    except KeyError:
        pass

    # create function and prevent llvm from optimizing it
    fnty = ir.FunctionType(ir.VoidType(), [arr.type, src_data.type, sz.type])
    func = ir.Function(module, fnty, fn_name)
    func.attributes.add('noinline')

    block = func.append_basic_block(name="entry")
    builder = ir.IRBuilder(block)
    sizeof_char = TargetInfo().sizeof('char')
    dest_data = builder.extract_value(builder.load(func.args[0]), [0])
    cgutils.raw_memcpy(builder, dest_data, func.args[1], func.args[2], sizeof_char)
    builder.ret_void()

    return func


@extending.lower_builtin(TextEncodingNone, nb_types.StringLiteral)
def heavydb_text_encoding_none_constructor_literal(context, builder, sig, args):
    int64_t = ir.IntType(64)
    int8_t_ptr = ir.IntType(8).as_pointer()

    literal_value = sig.args[0].literal_value
    sz = int64_t(len(literal_value))

    # arr = {ptr, size, is_null}*
    arr = heavydb_buffer_constructor(context, builder, sig.return_type(nb_types.int64), [sz])._getpointer()  # noqa: E501

    msg_bytes = literal_value.encode('utf-8')
    msg_const = cgutils.make_bytearray(msg_bytes + b'\0')
    try:
        msg_global_var = builder.module.get_global(literal_value)
    except KeyError:
        msg_global_var = cgutils.global_constant(builder.module, literal_value, msg_const)
    src_data = builder.bitcast(msg_global_var, int8_t_ptr)

    fn = get_copy_bytes_fn(builder.module, arr, src_data, sz)
    builder.call(fn, [arr, src_data, sz])
    return arr


@extending.type_callable(TextEncodingNone)
def type_heavydb_text_encoding_none(context):
    def typer(arg):
        if isinstance(arg, nb_types.UnicodeType):
            raise RequireLiteralValue('Requires StringLiteral')
        if isinstance(arg, (nb_types.Integer, nb_types.StringLiteral)):
            return typesystem.Type.fromobject('TextEncodingNone').tonumba()
    return typer


@extending.intrinsic
def ol_attr_ptr_(typingctx, text):
    sig = nb_types.voidptr(text)

    def codegen(context, builder, sig, args):
        [data] = args
        return builder.extract_value(builder.load(data), 0)

    return sig, codegen


@extending.intrinsic
def ol_attr_sz_(typingctx, text):
    sig = nb_types.int64(text)

    def codegen(context, builder, sig, args):
        [data] = args
        return builder.extract_value(builder.load(data), 1)

    return sig, codegen


@extending.intrinsic
def ol_attr_is_null_(typingctx, text):
    sig = nb_types.int8(text)

    def codegen(context, builder, sig, args):
        [data] = args
        return builder.extract_value(builder.load(data), 2)

    return sig, codegen


@extending.overload_attribute(TextEncodingNonePointer, 'ptr')
def ol_attr_ptr(text):
    def impl(text):
        return ol_attr_ptr_(text)
    return impl


@extending.overload_attribute(TextEncodingNonePointer, 'sz')
def ol_attr_sz(text):
    def impl(text):
        return ol_attr_sz_(text)
    return impl


@extending.overload_attribute(TextEncodingNonePointer, 'is_null')
def ol_attr_is_null(text):
    def impl(text):
        return ol_attr_is_null_(text)
    return impl
