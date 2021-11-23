
__all__ = ['StructureNumbaType', 'StructureNumbaPointerType', 'make_numba_struct']

import operator
from rbc.irutils import get_member_value
from llvmlite import ir
from numba.core import datamodel, extending, types, imputils, typing, cgutils, typeconv


typing_registry = typing.templates.builtin_registry
lowering_registry = imputils.builtin_registry

int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()
fp32 = ir.FloatType()
fp64 = ir.DoubleType()


class StructureTypeAttributeTemplate(typing.templates.AttributeTemplate):
    key = NotImplemented

    def generic_resolve(self, typ, attr):
        model = datamodel.default_manager.lookup(typ)
        return model.get_member_fe_type(attr)


class StructurePointerTypeAttributeTemplate(typing.templates.AttributeTemplate):
    key = NotImplemented

    def generic_resolve(self, typ, attr):
        model = datamodel.default_manager.lookup(typ.dtype)
        return model.get_member_fe_type(attr)


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
        return Type.fromnumba(self.dtype).pointer()

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        name = "%s[%s]*" % (type(self).__name__, dtype)
        super().__init__(name)

    @property
    def key(self):
        return self.dtype

    # Subclasses may redefine the following methods

    def get_add_impl(self):
        def impl(x, i):
            return StructureNumbaPointerType_add_impl(x, i)
        return impl

    def get_getitem_impl(self):
        return self.get_add_impl()


def make_numba_struct(name, members, origin=None, _cache={}):
    """Create numba struct type instance.
    """
    t = _cache.get(name)
    if t is None:
        base = None
        if origin is not None:
            assert origin.is_struct
            base = origin._params.get('NumbaType')
            numba_ptr_type = origin._params.get('NumbaPointerType', StructureNumbaPointerType)
        else:
            base = None
            numba_ptr_type = StructureNumbaPointerType
        if base is None:
            base = Type.aliases.get('StructureNumbaType', StructureNumbaType)
        assert issubclass(base, types.Type)  # base must be numba.types.Type
        assert issubclass(numba_ptr_type, types.Type)  # numba_ptr_type must be numba.types.Type

        struct_type = type(name+'Type', (base,),
                           dict(members=[t for n, t in members],
                                __typesystem_type__=origin))
        _cache[name] = t = struct_type(name)

        def model__init__(self, dmm, fe_type):
            datamodel.StructModel.__init__(self, dmm, fe_type, members)
        struct_model = type(name+'Model',
                            (datamodel.StructModel,),
                            dict(__init__=model__init__))
        struct_pointer_model = type(name+'PointerModel', (datamodel.models.PointerModel,), dict())

        datamodel.registry.register_default(struct_type)(struct_model)
        datamodel.registry.register_default(numba_ptr_type)(struct_pointer_model)

        if not issubclass(numba_ptr_type, StructureNumbaPointerType):
            return t

        # member access via attribute getters/setters
        struct_attr_template = type(name+'AttributeTemplate',
                                    (StructureTypeAttributeTemplate,),
                                    dict(key=struct_type))
        struct_pointer_attr_template = type(name+'PointerAttributeTemplate',
                                            (StructurePointerTypeAttributeTemplate,),
                                            dict(key=numba_ptr_type))
        typing_registry.register_attr(struct_pointer_attr_template)
        typing_registry.register_attr(struct_attr_template)

        # allow casting to/from voidptr
        tptr = numba_ptr_type(t)
        typeconv.rules.default_type_manager.set_compatible(
            tptr, types.voidptr, typeconv.Conversion.safe)
        typeconv.rules.default_type_manager.set_compatible(
            types.voidptr, tptr, typeconv.Conversion.safe)

    return t


@lowering_registry.lower_getattr_generic(StructureNumbaType)
def StructureNumbaType_getattr_impl(context, builder, sig, struct, attr):
    model = datamodel.default_manager.lookup(sig)
    index = model.get_field_position(attr)
    return get_member_value(builder, struct, index)


@lowering_registry.lower_setattr_generic(StructureNumbaType)
def StructureNumbaType_setattr_impl(context, builder, sig, args, attr):
    typ = sig.args[0]
    struct, value = args
    model = datamodel.default_manager.lookup(typ)
    index = model.get_field_position(attr)
    ptr = builder.gep(struct.operands[0], [int32_t(0), int32_t(index)])
    return builder.store(value, ptr)


@lowering_registry.lower_getattr_generic(StructureNumbaPointerType)
def StructureNumbaPointerType_getattr_impl(context, builder, sig, struct, attr):
    typ = sig.dtype
    model = datamodel.default_manager.lookup(typ)
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
def StructureNumbaPointerType_add(x, i):
    if isinstance(x, StructureNumbaPointerType):
        return x.get_add_impl()


@extending.overload(operator.getitem)
def StructureNumbaPointerType_getitem(x, i):
    if isinstance(x, StructureNumbaPointerType):
        return x.get_getitem_impl()


@lowering_registry.lower_cast(types.RawPointer, StructureNumbaPointerType)
@lowering_registry.lower_cast(StructureNumbaPointerType, types.RawPointer)
def impl_T_star_to_T_star(context, builder, fromty, toty, value):
    return builder.bitcast(value, Type.fromnumba(toty).tollvmir())


if 1:
    from rbc.typesystem import Type
