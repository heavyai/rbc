from llvmlite import ir
from numba.core import extending
from numba.core import types as nb_types
from numba.core.errors import TypingError
from rbc import irutils


@extending.intrinsic
def add_ints(typingctx, a_type, b_type):
    if (a_type, b_type) != (nb_types.int64, nb_types.int64):
        raise TypingError('add_ints(i64, i64)')

    sig = nb_types.int64(nb_types.int64, nb_types.int64)

    def codegen(context, builder, signature, args):
        assert len(args) == 2
        arg_a, arg_b = args
        int64_t = ir.IntType(64)
        fntype = ir.FunctionType(int64_t, [int64_t, int64_t])
        fn = irutils.get_or_insert_function(builder.module, fntype,
                                            name="_rbclib_add_ints")
        return builder.call(fn, [arg_a, arg_b])

    return sig, codegen
