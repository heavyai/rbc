from rbc import typesystem
from rbc.errors import RequireLiteralValue
from numba.core import extending
from numba import types as nb_types
from llvmlite import ir


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
