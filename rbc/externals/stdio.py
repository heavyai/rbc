"""This module provides the following tools:

Intrinsics:

  fflush()
  printf(<StringLiteral>, <arg0>, <arg1>, ..., <argN>)

Methods:

  cg_fflush(<llvmlite.ir.IRBuilder>)
"""

from llvmlite import ir
from rbc.targetinfo import TargetInfo
from numba.core import cgutils, extending
from numba.core import types as nb_types

int32_t = ir.IntType(32)


def cg_fflush(builder):
    int8_t = ir.IntType(8)
    fflush_fnty = ir.FunctionType(int32_t, [int8_t.as_pointer()])
    fflush_fn = builder.module.get_or_insert_function(fflush_fnty, name="fflush")

    builder.call(fflush_fn, [int8_t.as_pointer()(None)])


@extending.intrinsic
def fflush(typingctx):
    """fflush that can be called from Numba jit-decorated functions.

    Note: fflush is available only for CPU target.
    """
    sig = nb_types.void(nb_types.void)

    def codegen(context, builder, signature, args):
        target_info = TargetInfo()
        if target_info.is_cpu:
            cg_fflush(builder)

    return sig, codegen


@extending.intrinsic
def printf(typingctx, format_type, *args):
    """printf that can be called from Numba jit-decorated functions.

    Note: printf is available only for CPU target.
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
        raise TypeError(f"expected StringLiteral but got {type(format_type).__name__}")
