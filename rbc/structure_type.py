from rbc.utils import get_version
from llvmlite import ir
if get_version('numba') >= (0, 49):
    from numba.core import datamodel, extending, types, imputils, typing
else:
    from numba import datamodel, extending, types, typing

from numba.core.typing.templates import Registry

typing_registry = Registry()
lowering_registry = imputils.Registry()


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()
fp32 = ir.FloatType()
fp64 = ir.DoubleType()


class StructureNumbaType(types.Type):
    pass_by_value = True


@extending.infer_getattr
class StructAttribute(typing.templates.AttributeTemplate):
    key = StructureNumbaType

    def generic_resolve(self, typ, attr):
        model = datamodel.default_manager.lookup(typ)
        return model.get_member_fe_type(attr)


@lowering_registry.lower_getattr_generic(StructureNumbaType)
def StructureNumbaType_getattr_impl(context, builder, sig, struct, attr):
    print(f'getattr_impl({sig=}, {struct=}, {attr=})')
    model = datamodel.default_manager.lookup(sig)
    assert struct.opname == 'load'
    index = model.get_field_position(attr)

    ptr = builder.gep(struct.operands[0], [int32_t(0), int32_t(0)])
    res = builder.load(builder.gep(ptr, [int32_t(index)]))
    return res
    return builder.load(builder.gep(struct.operands[0], [int32_t(0), int32_t(index)]))


@lowering_registry.lower_setattr_generic(StructureNumbaType)
def StructureNumbaType_setattr_impl(context, builder, sig, args, attr):
    print(f'setattr_impl({sig=}, {args=}, {attr=})')
    typ = sig.args[0]
    struct, value = args
    print(f'{struct=} {value=}')
    model = datamodel.default_manager.lookup(typ)
    assert struct.opname == 'load'
    index = model.get_field_position(attr)
    print(f'{builder=}')
    ptr = builder.gep(struct.operands[0], [int32_t(0), int32_t(index)])
    return builder.store(value, ptr)
