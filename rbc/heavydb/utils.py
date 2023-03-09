from llvmlite import ir
from numba import types as nb_types
from numba.core import cgutils, extending

from rbc.errors import RequireLiteralValue

i8 = ir.IntType(8)
i8p = i8.as_pointer()


@extending.intrinsic
def deref(typingctx, x):
    sig = x.dtype(x)

    def codegen(context, builder, sig, args):
        [ptr] = args
        return builder.load(ptr)

    return sig, codegen


@extending.intrinsic
def ref(typingctx, obj):
    sig = nb_types.CPointer(obj)(obj)

    def codegen(context, builder, sig, args):
        [obj] = args
        fa = context.make_helper(builder, sig.args[0], value=obj)
        return fa._getpointer()

    return sig, codegen


@extending.intrinsic
def as_voidptr(typingctx, x):
    ptr = nb_types.CPointer(nb_types.int8)
    sig = ptr(x)

    def codegen(context, builder, sig, args):
        [ptr] = args
        return builder.bitcast(ptr, i8p)

    return sig, codegen


@extending.intrinsic
def get_alloca(typingctx, x):
    ptr = nb_types.CPointer(x)
    sig = ptr(x)

    def codegen(context, builder, sig, args):
        [val] = args
        fa = context.make_helper(builder, sig.args[0], value=val)
        return builder.bitcast(fa._getpointer(), i8p)

    return sig, codegen


@extending.intrinsic
def global_str_constant(typingctx, identifier, msg):
    ptr = nb_types.CPointer(nb_types.int8)
    sig = ptr(identifier, msg)

    if not isinstance(msg, nb_types.StringLiteral):
        raise RequireLiteralValue(f"expected StringLiteral but got {type(msg).__name__}")

    def codegen(context, builder, sig, args):
        msg_bytes = msg.literal_value.encode('utf-8')
        msg_const = cgutils.make_bytearray(msg_bytes + b'\0')
        msg_global_var = cgutils.global_constant(
            builder.module,
            identifier.literal_value,
            msg_const)
        msg_ptr = builder.bitcast(msg_global_var, i8p)
        return msg_ptr

    return sig, codegen
