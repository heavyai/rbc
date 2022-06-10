"""
RBC TextEncodingNone type that corresponds to HeavyDB type TEXT ENCODED NONE.
"""

__all__ = ['TextEncodingNonePointer', 'TextEncodingNone', 'HeavyDBTextEncodingNoneType']

import operator
from rbc import typesystem
from rbc.targetinfo import TargetInfo
from rbc.errors import RequireLiteralValue
from .buffer import (
    BufferPointer, Buffer, HeavyDBBufferType,
    heavydb_buffer_constructor)
from numba.core import types, extending, cgutils
from llvmlite import ir
from typing import Union


class TextEncodingNonePointer(BufferPointer):
    pass


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


class TextEncodingNone(Buffer):
    """
    RBC ``TextEncodingNone`` type that corresponds to HeavyDB type TEXT ENCODED NONE.
    HeavyDB TextEncodingNone represents the following structure:

    .. code-block:: c

        struct TextEncodingNone {
            char* ptr;
            size_t sz;  // when non-negative, TextEncodingNone has fixed width.
            int8_t is_null;
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
    elif isinstance(a, TextEncodingNonePointer) and isinstance(b, types.StringLiteral):
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
        if isinstance(b, (TextEncodingNonePointer, types.StringLiteral)):
            def impl(a, b):
                return not(a == b)
            return impl


@extending.lower_builtin(TextEncodingNone, types.Integer)
def heavydb_text_encoding_none_constructor(context, builder, sig, args):
    return heavydb_buffer_constructor(context, builder, sig, args)


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


@extending.lower_builtin(TextEncodingNone, types.StringLiteral)
def heavydb_text_encoding_none_constructor_literal(context, builder, sig, args):
    int64_t = ir.IntType(64)
    int8_t_ptr = ir.IntType(8).as_pointer()

    literal_value = sig.args[0].literal_value
    sz = int64_t(len(literal_value))

    # arr = {ptr, size, is_null}*
    arr = heavydb_buffer_constructor(context, builder, sig.return_type(types.int64), [sz])

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
        if isinstance(arg, types.UnicodeType):
            raise RequireLiteralValue('Requires StringLiteral')
        if isinstance(arg, (types.Integer, types.StringLiteral)):
            return typesystem.Type.fromobject('TextEncodingNone').tonumba()
    return typer
