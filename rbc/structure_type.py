import operator
from rbc.utils import get_version
from llvmlite import ir
from rbc.typesystem import Type

if get_version('numba') >= (0, 49):
    from numba.core import datamodel, extending, types, imputils, typing, cgutils, typeconv
else:
    from numba import datamodel, extending, types, typing, typeconv

from numba.core.typing.templates import Registry

typing_registry = Registry()
lowering_registry = imputils.Registry()

int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()
fp32 = ir.FloatType()
fp64 = ir.DoubleType()


def make_numba_struct(name, members, base=None, origin=None, _cache={}):
    """Create numba struct type instance.
    """
    t = _cache.get(name)
    if t is None:
        if base is None:
            base = Type.aliases.get('StructureNumbaType', StructureNumbaType)
        assert issubclass(base, types.Type)  # base must be numba.types.Type

        def model__init__(self, dmm, fe_type):
            datamodel.StructModel.__init__(self, dmm, fe_type, members)

        struct_model = type(name+'Model',
                            (datamodel.StructModel,),
                            dict(__init__=model__init__))
        struct_type = type(name+'Type', (base,),
                           dict(members=[t for n, t in members], origin=origin,
                                __typesystem_type__=origin))
        datamodel.registry.register_default(struct_type)(struct_model)
        _cache[name] = t = struct_type(name)

        tptr = StructureNumbaPointerType(t)
        typeconv.rules.default_type_manager.set_compatible(
            tptr, types.voidptr, typeconv.Conversion.safe)
        typeconv.rules.default_type_manager.set_compatible(
            types.voidptr, tptr, typeconv.Conversion.safe)

    return t


class StructureNumbaType(types.Type):
    """Represents a struct numba type.
    """


class StructureNumbaPointerType(types.Type):
    """Pointer type for StructureNumbaType values.

    We are not deriving from CPointer because we may want to use
    getitem for custom item access.
    """

    @property
    def __typesystem_type__(self):
        return self.dtype.origin.pointer()

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        name = "(%s)*" % dtype
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


@datamodel.register_default(StructureNumbaPointerType)
class StructureNumbaPointerTypeModel(datamodel.models.PointerModel):
    pass


@typing_registry.register_attr
class StructAttribute(typing.templates.AttributeTemplate):
    key = StructureNumbaType

    def generic_resolve(self, typ, attr):
        model = datamodel.default_manager.lookup(typ)
        return model.get_member_fe_type(attr)


@typing_registry.register_attr
class StructPointerAttribute(typing.templates.AttributeTemplate):
    key = StructureNumbaPointerType

    def generic_resolve(self, typ, attr):
        model = datamodel.default_manager.lookup(typ.dtype)
        return model.get_member_fe_type(attr)


@lowering_registry.lower_getattr_generic(StructureNumbaType)
def StructureNumbaType_getattr_impl(context, builder, sig, struct, attr):
    model = datamodel.default_manager.lookup(sig)
    assert struct.opname == 'load'
    index = model.get_field_position(attr)

    ptr = builder.gep(struct.operands[0], [int32_t(0), int32_t(0)])
    return builder.load(builder.gep(ptr, [int32_t(index)]))


@lowering_registry.lower_setattr_generic(StructureNumbaType)
def StructureNumbaType_setattr_impl(context, builder, sig, args, attr):
    typ = sig.args[0]
    struct, value = args
    model = datamodel.default_manager.lookup(typ)
    assert struct.opname == 'load'
    index = model.get_field_position(attr)
    ptr = builder.gep(struct.operands[0], [int32_t(0), int32_t(index)])
    return builder.store(value, ptr)


@lowering_registry.lower_getattr_generic(StructureNumbaPointerType)
def StructureNumbaPointerType_getattr_impl(context, builder, sig, struct, attr):
    typ = sig.dtype
    model = datamodel.default_manager.lookup(typ)
    assert struct.opname == 'load'
    index = model.get_field_position(attr)
    rawptr = cgutils.alloca_once_value(builder, value=struct)
    struct = builder.load(builder.gep(rawptr, [int32_t(0)]))
    return builder.load(builder.gep(
        struct, [int32_t(0), int32_t(index)]))


@lowering_registry.lower_setattr_generic(StructureNumbaPointerType)
def StructureNumbaPointerType_setattr_impl(context, builder, sig, args, attr):
    typ = sig.args[0].dtype
    struct, value = args
    model = datamodel.default_manager.lookup(typ)
    assert struct.opname == 'load'
    index = model.get_field_position(attr)
    rawptr = cgutils.alloca_once_value(builder, value=struct)
    ptr = builder.load(rawptr)
    buf = builder.load(builder.gep(ptr, [int32_t(0), int32_t(index)]))
    builder.store(value, buf.operands[0])


@extending.intrinsic
def StructureNumbaPointerType_add_impl(typingctx, data, index):
    sig = data(data, index)
    ptr_type = data

    def codegen(context, builder, signature, args):
        ll_ptr_type = context.get_value_type(ptr_type)
        ll_value_type = context.get_value_type(ptr_type.dtype)
        size = context.get_abi_sizeof(ll_value_type)
        ptr, index = args

        i = builder.ptrtoint(ptr, int64_t)
        offset = builder.mul(index, int64_t(size))
        new_i = builder.add(i, offset)

        return builder.inttoptr(new_i, ll_ptr_type)
    return sig, codegen


@extending.overload(operator.add)
@extending.overload(operator.getitem)
def StructureNumbaPointerType_add(x, i):
    if isinstance(x, StructureNumbaPointerType):

        def impl(x, i):
            return StructureNumbaPointerType_add_impl(x, i)
        return impl


@lowering_registry.lower_cast(types.RawPointer, StructureNumbaPointerType)
@lowering_registry.lower_cast(StructureNumbaPointerType, types.RawPointer)
def impl_T_star_to_T_star(context, builder, fromty, toty, value):
    return builder.bitcast(value, Type.fromnumba(toty).tollvmir())
