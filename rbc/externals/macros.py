"""This module provides the following tools:

Intrinsics:

  sizeof(<numba.types.TypeRef|Type instance>)
  cast(<numba.types.voidptr instance>, <numba.types.TypeRef|StringLiteral instance>)

Methods:

  <numba.types.voidptr instance>.cast(<numba.types.TypeRef|StringLiteral instance>)

Casts:

  intp, voidptr to/from T*, T**, ..., T************* where T is a scalar type.
"""

__all__ = ["sizeof", "cast"]

from llvmlite import ir
from numba.core import extending, imputils, typing, typeconv
from numba.core import types as nb_types
from rbc.typesystem import Type


typing_registry = typing.templates.builtin_registry
lowering_registry = imputils.builtin_registry


@extending.intrinsic
def sizeof(typingctx, arg_type):
    """Return sizeof type."""
    if isinstance(arg_type, nb_types.TypeRef):
        arg_val_type = arg_type.key
    elif isinstance(arg_type, nb_types.Type):
        arg_val_type = arg_type
    else:
        return
    sig = nb_types.int32(arg_type)

    def codegen(context, builder, signature, args):
        value_type = context.get_value_type(arg_val_type)
        size = context.get_abi_sizeof(value_type)
        return ir.Constant(ir.IntType(32), size)

    return sig, codegen


@extending.intrinsic
def cast(typingctx, ptr, typ):
    """Cast pointer value to any pointer type."""
    if isinstance(typ, nb_types.StringLiteral):
        dtype = Type.fromstring(typ.literal_value)
    elif isinstance(typ, nb_types.TypeRef):
        dtype = Type.fromnumba(typ.key)
    else:
        return
    assert dtype.is_pointer
    sig = dtype.tonumba()(ptr, typ)

    def codegen(context, builder, signature, args):
        return builder.bitcast(args[0], dtype.tollvmir())

    return sig, codegen


@extending.overload_method(type(nb_types.voidptr), "cast")
def voidptr_cast_to_any(ptr, typ):
    """Convenience method for casting voidptr to any pointer type."""
    if isinstance(ptr, type(nb_types.voidptr)):
        if isinstance(typ, (nb_types.TypeRef, nb_types.StringLiteral)):

            def impl(ptr, typ):
                return cast(ptr, typ)

            return impl


@lowering_registry.lower_cast(nb_types.intp, nb_types.CPointer)
def impl_intp_to_T_star(context, builder, fromty, toty, value):
    return builder.inttoptr(value, Type.fromnumba(toty).tollvmir())


@lowering_registry.lower_cast(nb_types.CPointer, nb_types.intp)
@lowering_registry.lower_cast(nb_types.RawPointer, nb_types.intp)
def impl_T_star_to_intp(context, builder, fromty, toty, value):
    return builder.ptrtoint(value, ir.IntType(toty.bitwidth))


@lowering_registry.lower_cast(nb_types.CPointer, nb_types.RawPointer)
@lowering_registry.lower_cast(nb_types.RawPointer, nb_types.CPointer)
def impl_T_star_to_T_star(context, builder, fromty, toty, value):
    return builder.bitcast(value, Type.fromnumba(toty).tollvmir())


scalar_types = [
    getattr(nb_types, typename)
    for typename in [
        "int64",
        "int32",
        "int16",
        "int8",
        "uint64",
        "uint32",
        "uint16",
        "uint8",
        "float32",
        "float64",
        "boolean",
    ]
]
pointer_value_types = [nb_types.intp, nb_types.voidptr]
pointer_types = [nb_types.CPointer(s) for s in scalar_types] + [nb_types.voidptr]
for p1 in pointer_value_types:
    for p2 in pointer_types:
        # ANSI C requires 12 as a maximum supported pointer level.
        for i in range(12):
            if p1 != p2:
                typeconv.rules.default_type_manager.set_compatible(
                    p1, p2, typeconv.Conversion.safe
                )
                typeconv.rules.default_type_manager.set_compatible(
                    p2, p1, typeconv.Conversion.safe
                )
            p2 = nb_types.CPointer(p2)
