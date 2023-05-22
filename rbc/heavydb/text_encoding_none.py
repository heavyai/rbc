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

from .allocator import allocate_varlen_buffer
from .array import ArrayPointer
from .buffer import Buffer, HeavyDBBufferType, heavydb_buffer_constructor

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

    def postprocess_type(self):
        return self.params(typename='TextEncodingNone')

    def tostring(self, use_typename=False, use_annotation=True, use_name=True,
                 use_annotation_name=False, _skip_annotation=False):
        use_typename = True
        return super().tostring(use_typename, use_annotation, use_name,
                                use_annotation_name, _skip_annotation)

    def match(self, other):
        if type(self) is type(other):
            return self[0] == other[0]
        if other.is_pointer and other[0].is_char and other[0].bits == 8:
            return 1
        if other.is_string:
            return 2


class TextEncodingNonePointer(ArrayPointer):
    def deepcopy(self, context, builder, val, retptr):
        zero, one, two = i32(0), i32(1), i32(2)

        src = builder.load(builder.gep(val, [zero, zero]))
        sz = builder.load(builder.gep(val, [zero, one]))
        is_null = builder.load(builder.gep(val, [zero, two]))

        dst = allocate_varlen_buffer(builder, sz, i64(1))
        cgutils.raw_memcpy(builder, dst=dst, src=src, count=sz, itemsize=i64(1))
        # string is null terminated
        builder.store(i8(0), builder.gep(dst, [sz]))

        builder.store(dst, builder.gep(retptr, [zero, zero]))
        builder.store(sz, builder.gep(retptr, [zero, one]))
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
    return heavydb_buffer_constructor(context, builder, sig, args)


@extending.lower_builtin(TextEncodingNone, nb_types.CPointer, nb_types.Integer)
def heavydb_text_encoding_none_constructor_memcpy(context, builder, sig, args):
    [ptr, sz] = args
    fa = heavydb_buffer_constructor(context, builder, sig.return_type(nb_types.int64), [sz])
    fa_ptr = builder.load(builder.gep(fa, [i32(0), i32(0)]))
    cgutils.memcpy(builder, fa_ptr, ptr, sz)
    # string is null terminated
    builder.store(fa_ptr.type.pointee(0), builder.gep(fa_ptr, [sz]))
    return fa


@extending.lower_builtin(TextEncodingNone, nb_types.UnicodeType)
def text_encoding_none_unicode_ctor(context, builder, sig, args):
    # Note to the future:
    # You shall be tempted to use "context.make_helper",
    # "cgutils.make_struct_proxy" or "builder.extract_value", but avoid it at
    # all cost! LLVM -O2 runs SROA (Scalar Replacement Of Aggregates) and
    # suddenly some IR nodes becomes NULL values which are deleted by DCE.
    # Stick to plain allocas + load/store and you'll be fine.

    # It seems to be safe to use context.make_helper on a previous allocated
    # memory!
    uni_str = context.make_helper(builder, sig.args[0], value=args[0])

    llty = context.get_value_type(sig.return_type.dtype)
    st_size = context.get_abi_sizeof(llty)
    st_ptr = builder.bitcast(allocate_varlen_buffer(builder, int64_t(st_size),
                                                    int64_t(1)),
                             llty.as_pointer())
    zero, one, two = i32(0), i32(1), i32(2)

    eq = builder.icmp_signed('==', uni_str.length, uni_str.length.type(0))
    with builder.if_else(eq, likely=False) as (then, otherwise):
        with then:
            bb_then = builder.basic_block
            p1 = cgutils.get_null_value(i8p)
            n1 = i8(1)
        with otherwise:
            bb_else = builder.basic_block
            p2 = allocate_varlen_buffer(builder, uni_str.length, i64(1))
            cgutils.raw_memcpy(builder, dst=p2, src=uni_str.data,
                               count=uni_str.length, itemsize=i64(1))
            # string is null terminated
            builder.store(i8(0), builder.gep(p2, [uni_str.length]))
            n2 = i8(0)

    # {i8*, i64, i8}
    #  ^^^
    ptr = builder.phi(i8p)
    ptr.add_incoming(p1, bb_then)
    ptr.add_incoming(p2, bb_else)

    # {i8*, i64, i8}
    #            ^^
    is_null = builder.phi(i8)
    is_null.add_incoming(n1, bb_then)
    is_null.add_incoming(n2, bb_else)

    builder.store(ptr, builder.gep(st_ptr, [zero, zero]))
    builder.store(uni_str.length, builder.gep(st_ptr, [zero, one]))
    builder.store(is_null, builder.gep(st_ptr, [zero, two]))
    return st_ptr


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


@extending.overload_method(TextEncodingNonePointer, 'is_null')
def ol_attr_is_null(text):
    def impl(text):
        return text.sz == 0
    return impl


@extending.overload_method(TextEncodingNonePointer, 'to_string')
def ol_to_string(text):
    def impl(text):
        kind = PY_UNICODE_1BYTE_KIND  # ASCII characters only
        length = len(text)
        is_ascii = True
        s = _empty_string(kind, length, is_ascii)
        for i in range(length):
            ch = text[i]
            _set_code_point(s, i, ch)
        return s
    return impl
