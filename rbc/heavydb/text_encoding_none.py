"""
RBC TextEncodingNone type that corresponds to HeavyDB type TEXT ENCODED NONE.
"""

__all__ = ['TextEncodingNonePointer', 'TextEncodingNone', 'HeavyDBTextEncodingNoneType']

import operator
from typing import Union

from llvmlite import ir
from numba.core import cgutils, extending
from numba.core import types as nb_types
from numba.core.pythonapi import PY_UNICODE_1BYTE_KIND
from numba.cpython.unicode import _empty_string, _set_code_point

from rbc import typesystem
from rbc.targetinfo import TargetInfo

from .array import ArrayPointer
from .buffer import Buffer, HeavyDBBufferType, heavydb_buffer_constructor
from .allocator import allocate_varlen_buffer

i8 = ir.IntType(8)
i8p = i8.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)
void = ir.VoidType()
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
        struct_load = builder.load(val)
        fa = context.make_helper(builder, self.dtype, value=struct_load)

        zero, one, two = i32(0), i32(1), i32(2)

        ptr = allocate_varlen_buffer(builder, fa.sz)
        # fn = get_copy_bytes_fn(builder)
        # builder.call(fn, [ptr, fa.ptr, fa.sz])
        cgutils.raw_memcpy(builder,
                           dst=ptr,
                           src=fa.ptr,
                           count=fa.sz,
                           itemsize=i64(1))
        builder.store(ptr, builder.gep(retptr, [zero, zero]))
        builder.store(fa.sz, builder.gep(retptr, [zero, one]))
        builder.store(fa.is_null, builder.gep(retptr, [zero, two]))


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

    def to_string(self) -> str:
        """
        Returns a Python string
        """
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


def get_copy_bytes_fn(builder):

    module = builder.module

    name = 'copy_bytes_fn'
    fnty = ir.FunctionType(void, [i8p, i8p, i64])
    fn = cgutils.get_or_insert_function(module, fnty, name)
    fn.linkage = 'linkonce'
    fn.attributes.add('noinline')

    block = fn.append_basic_block(name="entry")
    builder = ir.IRBuilder(block)
    sizeof_char = TargetInfo().sizeof('char')
    [dst, src, size] = fn.args
    cgutils.raw_memcpy(builder, dst, src, size, sizeof_char)
    builder.ret_void()

    return fn


@extending.lower_builtin(TextEncodingNone, nb_types.CPointer, nb_types.Integer)
def heavydb_text_encoding_none_constructor_memcpy(context, builder, sig, args):
    [ptr, sz] = args
    fa = heavydb_buffer_constructor(context, builder, sig.return_type(nb_types.int64), [sz])
    cgutils.memcpy(builder, fa.ptr, ptr, sz)
    # string is null terminated
    builder.store(fa.ptr.type.pointee(0), builder.gep(fa.ptr, [sz]))
    return fa._getpointer()


@extending.lower_builtin(TextEncodingNone, nb_types.UnicodeType)
def text_encoding_none_unicode_ctor(context, builder, sig, args):
    uni_str = context.make_helper(builder, sig.args[0], value=args[0])
    fa = heavydb_buffer_constructor(context, builder, sig.return_type(nb_types.int64),
                                    [uni_str.length])
    # ret.ptr = allocate_varlen_buffer(builder, uni_str.length)
    # ret.sz = uni_str.length
    # ret.is_null = i8(0)

    # fn = get_copy_bytes_fn(builder)
    # builder.call(fn, [fa.ptr, uni_str.data, uni_str.length])

    cgutils.raw_memcpy(builder,
                       dst=fa.ptr,
                       src=uni_str.data,
                       count=uni_str.length,
                       itemsize=i64(1))
    # string is null terminated
    builder.store(fa.ptr.type.pointee(0), builder.gep(fa.ptr, [uni_str.length]))
    return fa._getpointer()


@extending.type_callable(TextEncodingNone)
def type_heavydb_text_encoding_none(context):
    def typer(arg):
        if isinstance(arg, (nb_types.Integer, nb_types.UnicodeType)):
            return typesystem.Type.fromobject('TextEncodingNone').tonumba()
    return typer


@extending.type_callable(TextEncodingNone)
def type_heavydb_text_encoding_none_typer2(context):
    def typer(ptr, sz):
        if isinstance(ptr, nb_types.CPointer) and isinstance(sz, nb_types.Integer):
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


@extending.overload_method(TextEncodingNonePointer, 'to_string')
def ol_to_string(text):
    def impl(text):
        kind = PY_UNICODE_1BYTE_KIND
        length = len(text)
        is_ascii = True
        s = _empty_string(kind, length, is_ascii)
        for i in range(length):
            ch = text[i]
            _set_code_point(s, i, ch)
        return s
    return impl
