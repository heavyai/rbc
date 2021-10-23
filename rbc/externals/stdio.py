"""https://en.cppreference.com/w/c/io
"""

from rbc import irutils
from llvmlite import ir
from rbc.targetinfo import TargetInfo
from numba.core import cgutils, extending
from numba.core import types as nb_types
from rbc.errors import NumbaTypeError  # some errors are available for Numba >= 0.55

int32_t = ir.IntType(32)


def cg_fflush(builder):
    int8_t = ir.IntType(8)
    fflush_fnty = ir.FunctionType(int32_t, [int8_t.as_pointer()])
    fflush_fn = irutils.get_or_insert_function(builder.module, fflush_fnty, name="fflush")

    builder.call(fflush_fn, [int8_t.as_pointer()(None)])


@extending.intrinsic
def fflush(typingctx):
    """``fflush`` that can be called from Numba jit-decorated functions.

    .. note::
        ``fflush`` is available only for CPU target.
    """
    sig = nb_types.void(nb_types.void)

    def codegen(context, builder, signature, args):
        target_info = TargetInfo()
        if target_info.is_cpu:
            cg_fflush(builder)

    return sig, codegen


@extending.intrinsic
def printf(typingctx, format_type, *args):
    """``printf`` that can be called from Numba jit-decorated functions.

    .. note::
        ``printf`` is available only for CPU target.
    """

    if isinstance(format_type, nb_types.StringLiteral):
        sig = nb_types.void(format_type, nb_types.BaseTuple.from_types(args))

        def codegen(context, builder, signature, args):
            target_info = TargetInfo()
            if target_info.is_cpu:
                cgutils.printf(builder, format_type.literal_value, *args[1:])
                cg_fflush(builder)

        return sig, codegen

    else:
        raise NumbaTypeError(f"expected StringLiteral but got {type(format_type).__name__}")
