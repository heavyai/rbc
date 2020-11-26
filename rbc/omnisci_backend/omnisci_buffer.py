"""Implement Buffer type as a base class to Omnisci Array and Column types.

Omnisci Buffer represents the following structure:

  template<typename T>
  struct Buffer {
    T* ptr;
    size_t sz;
    ...
  }

that is, a structure that has at least two members where the first is
a pointer to some data type and the second is the size of the buffer.

This module implements the support for the following Python operators:

  len
  __getitem__
  __setitem__

to provide a minimal functionality for accessing and manipulating the
Omnisci buffer objects from UDF/UDTFs.
"""


import operator
from collections import defaultdict
from llvmlite import ir
from rbc import typesystem
from rbc.utils import get_version
from llvmlite import ir as llvm_ir
if get_version('numba') >= (0, 49):
    from numba.core import datamodel, cgutils, extending, types
else:
    from numba import datamodel, cgutils, extending, types


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()


class OmnisciBufferType(typesystem.Type):
    """Typesystem type class for Omnisci buffer structures.
    """

    @property
    def _buffer_typename(self):
        return self._params['typename']

    def pointer(self):
        ptr_type = self._params.get('pointer')
        if ptr_type is not None:
            return ptr_type
        return typesystem.Type.pointer(self)


class BufferType(types.Type):
    """Numba type class for Omnisci buffer structures.
    """

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.members[0].dtype


class BufferPointer(types.Type):
    """Numba type class for pointers to Omnisci buffer structures.

    We are not deriving from CPointer because BufferPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype, eltype):
        self.dtype = dtype    # struct dtype
        self.eltype = eltype  # buffer element dtype
        name = "(%s)*" % dtype
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


class BufferMeta(type):

    class_names = set()

    def __init__(cls, name, bases, dct):
        type(cls).class_names.add(name)


class Buffer(object, metaclass=BufferMeta):
    """Represents Omnisci Buffer that can be constructed within UDF/UDTFs.
    """


class BufferPointerModel(datamodel.models.PointerModel):
    """Base class for Omnisci buffer pointer models.

    Subclasses should register the model for the corresponding
    BufferPointer subclass, for instance::

      @datamodel.register_default(ArrayPointer)
      class ArrayPointerModel(BufferPointerModel):
          pass
    """


builder_buffers = defaultdict(list)


def omnisci_buffer_constructor(context, builder, sig, args):
    """

    Usage:

      extending.lower_builtin(MyBuffer, numba.types.Integer, ...)(omnisci_buffer_constructor)

    will enable creating MyBuffer intance from a Omnisci UDF/UDTF definition:

      b = MyBuffer(<size>, ...)

    """
    ptr_type, sz_type = sig.return_type.dtype.members[:2]
    if len(sig.return_type.dtype.members) > 2:
        assert len(sig.return_type.dtype.members) == 3
        null_type = sig.return_type.dtype.members[2]
    else:
        null_type = None
    assert isinstance(args[0].type, ir.IntType), (args[0].type)
    element_count = builder.zext(args[0], int64_t)
    element_size = int64_t(ptr_type.dtype.bitwidth // 8)

    alloc_fnty = ir.FunctionType(int8_t.as_pointer(), [int64_t, int64_t])

    # TODO: use omnisci alloc function instead of calloc, see rbc issue 87
    alloc_fn = builder.module.get_or_insert_function(
        alloc_fnty, name="calloc")
    ptr8 = builder.call(alloc_fn, [element_count, element_size])
    # remember possible temporary allocations so that when leaving a
    # UDF/UDTF, these will be deallocated, see omnisci_pipeline.py.
    builder_buffers[builder].append(ptr8)
    ptr = builder.bitcast(ptr8, context.get_value_type(ptr_type))

    fa = cgutils.create_struct_proxy(sig.return_type.dtype)(context, builder)
    fa.ptr = ptr                  # T*
    fa.sz = element_count         # size_t
    if null_type is not None:
        is_null = context.get_value_type(null_type)(0)
        fa.is_null = is_null      # int8_t
    return fa._getpointer()


@extending.intrinsic
def free_omnisci_buffer(typingctx, ret):
    sig = types.void(ret)

    def codegen(context, builder, signature, args):
        buffers = builder_buffers[builder]

        # TODO: using stdlib `free` that works only for CPU. For CUDA
        # devices, we need to use omniscidb provided deallocator.
        free_fnty = llvm_ir.FunctionType(void_t, [int8_t.as_pointer()])
        free_fn = builder.module.get_or_insert_function(free_fnty, name="free")

        # We skip the ret pointer iff we're returning a Buffer
        # otherwise, we free everything
        if isinstance(ret, BufferPointer):
            [arg] = args
            ptr = builder.load(builder.gep(arg, [int32_t(0), int32_t(0)]))
            ptr = builder.bitcast(ptr, int8_t.as_pointer())
            for ptr8 in buffers:
                with builder.if_then(builder.icmp_signed('!=', ptr, ptr8)):
                    builder.call(free_fn, [ptr8])
        else:
            for ptr8 in buffers:
                builder.call(free_fn, [ptr8])

        del builder_buffers[builder]

    return sig, codegen


@extending.intrinsic
def omnisci_buffer_ptr_len_(typingctx, data):
    sig = types.int64(data)

    def codegen(context, builder, signature, args):
        data, = args
        rawptr = cgutils.alloca_once_value(builder, value=data)
        struct = builder.load(builder.gep(rawptr,
                                          [int32_t(0)]))
        return builder.load(builder.gep(
            struct, [int32_t(0), int32_t(1)]))
    return sig, codegen


@extending.intrinsic
def omnisci_buffer_len_(typingctx, data):
    sig = types.int64(data)

    def codegen(context, builder, signature, args):
        data, = args
        assert data.opname == 'load'
        struct = data.operands[0]
        return builder.load(builder.gep(
            struct, [int32_t(0), int32_t(1)]))

    return sig, codegen


@extending.overload(len)
def omnisci_buffer_len(x):
    if isinstance(x, BufferPointer):
        return lambda x: omnisci_buffer_ptr_len_(x)
    if isinstance(x, BufferType):
        return lambda x: omnisci_buffer_len_(x)


@extending.intrinsic
def omnisci_buffer_ptr_getitem_(typingctx, data, index):
    sig = data.eltype(data, index)

    def codegen(context, builder, signature, args):
        data, index = args
        rawptr = cgutils.alloca_once_value(builder, value=data)
        buf = builder.load(builder.gep(rawptr, [int32_t(0)]))
        ptr = builder.load(builder.gep(
            buf, [int32_t(0), int32_t(0)]))
        res = builder.load(builder.gep(ptr, [index]))

        return res
    return sig, codegen


@extending.intrinsic
def omnisci_buffer_getitem_(typingctx, data, index):
    eltype = data.eltype
    sig = eltype(data, index)

    def codegen(context, builder, signature, args):
        data, index = args
        assert data.opname == 'load'
        buf = data.operands[0]

        ptr = builder.load(builder.gep(
            buf, [int32_t(0), int32_t(0)]))
        res = builder.load(builder.gep(ptr, [index]))

        return res
    return sig, codegen


@extending.overload(operator.getitem)
def omnisci_buffer_getitem(x, i):
    if isinstance(x, BufferPointer):
        return lambda x, i: omnisci_buffer_ptr_getitem_(x, i)
    if isinstance(x, BufferType):
        return lambda x, i: omnisci_buffer_getitem_(x, i)


# [rbc issue-197] Numba promotes operations like
# int32(a) + int32(b) to int64
def truncate_or_extend(builder, nb_value, eltype, value, buf_typ):
    # buf[pos] = val

    if isinstance(nb_value, types.Integer):  # Integer
        if eltype.bitwidth < nb_value.bitwidth:
            return builder.trunc(value, buf_typ)  # truncate
        elif eltype.bitwidth > nb_value.bitwidth:
            is_signed = nb_value.signed
            return builder.sext(value, buf_typ) if is_signed else \
                builder.zext(value, buf_typ)  # extend
    elif isinstance(nb_value, types.Float):  # Floating-point
        if eltype.bitwidth < nb_value.bitwidth:
            return builder.fptrunc(value, buf_typ)  # truncate
        elif eltype.bitwidth > nb_value.bitwidth:
            return builder.fpext(value, buf_typ)  # extend
    elif isinstance(nb_value, types.Boolean):
        if buf_typ.width < value.type.width:
            return builder.trunc(value, buf_typ)
        elif buf_typ.width > value.type.width:
            return builder.sext(value, buf_typ)

    return value


@extending.intrinsic
def omnisci_buffer_ptr_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    eltype = data.eltype
    nb_value = value

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index, value = args

        rawptr = cgutils.alloca_once_value(builder, value=data)
        ptr = builder.load(rawptr)

        buf = builder.load(builder.gep(ptr, [zero, zero]))
        value = truncate_or_extend(builder, nb_value, eltype, value, buf.type.pointee)
        builder.store(value, builder.gep(buf, [index]))

    return sig, codegen


@extending.intrinsic
def omnisci_buffer_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    eltype = data.members[0].dtype
    nb_value = value

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index, value = args

        assert data.opname == 'load'
        buf = data.operands[0]

        ptr = builder.load(builder.gep(
            buf, [zero, zero]))

        value = truncate_or_extend(builder, nb_value, eltype, value, ptr.type.pointee)
        builder.store(value, builder.gep(ptr, [index]))

    return sig, codegen


@extending.overload(operator.setitem)
def omnisci_buffer_setitem(a, i, v):
    if isinstance(a, BufferPointer):
        return lambda a, i, v: omnisci_buffer_ptr_setitem_(a, i, v)
    if isinstance(a, BufferType):
        return lambda a, i, v: omnisci_buffer_setitem_(a, i, v)


@extending.intrinsic
def omnisci_buffer_is_null_(typingctx, data):
    sig = types.int8(data)

    def codegen(context, builder, signature, args):

        rawptr = cgutils.alloca_once_value(builder, value=args[0])
        ptr = builder.load(rawptr)

        return builder.load(builder.gep(ptr, [int32_t(0), int32_t(2)]))

    return sig, codegen


@extending.overload_method(BufferPointer, 'is_null')
def omnisci_buffer_is_null(x):
    if isinstance(x, BufferPointer):
        def impl(x):
            return omnisci_buffer_is_null_(x)
        return impl


def make_buffer_ptr_type(buffer_type, buffer_ptr_cls):
    ptr_type = buffer_type.pointer()

    numba_type = buffer_type.tonumba(bool_is_int8=True)
    numba_eltype = buffer_type[0][0].tonumba(bool_is_int8=True)
    numba_type_ptr = buffer_ptr_cls(numba_type, numba_eltype)

    ptr_type._params['tonumba'] = numba_type_ptr
    ptr_type._params['typename'] = buffer_type._params['typename'] + '*'
    return ptr_type


def buffer_type_converter(obj, typesystem_buffer_type,
                          buffer_typename, buffer_ptr_cls,
                          extra_members=[], element_type=None):
    """Template function for converting typesystem Type instances to
    Omnisci Buffer Type instance.

    Parameters
    ----------
    obj : {str, typesystem.Type}
      Custom atomic type corresponding to Omnisci Buffer.
    typesystem_buffer_type : {OmnisciBufferType subclass}
    buffer_typename : str
      The name of buffer type.
    buffer_ptr_cls : {BufferPointer subclass}
    extra_members : {list of typesystem Type instances}
      Additional members of the buffer structure.
    element_type : {None, typesystem Type instance}
      Specify element type. Otherwise deduce it from `obj` parameters.
    """
    if isinstance(obj, str):
        obj = typesystem.Type(obj,)

    if not isinstance(obj, typesystem.Type):
        raise NotImplementedError(type(obj))

    # A valid obj for buffer type is in the form
    #   typesystem.Type('BufferTypeName<ElementType>', )
    # In all other cases refuse by returning None.

    if element_type is None:
        name, params = obj.get_name_and_parameters()
        if name is None or name != buffer_typename:
            return
        assert len(params) == 1, params
        element_type = typesystem.Type.fromstring(params[0])
        if not element_type.is_concrete:
            return
        typename = '%s<%s>' % (buffer_typename, element_type.toprototype())
    else:
        element_type = typesystem.Type.fromobject(element_type)
        typename = buffer_typename

    assert issubclass(buffer_ptr_cls, BufferPointer)
    assert issubclass(typesystem_buffer_type, OmnisciBufferType)

    # Construct buffer type as a struct with ptr and sz as members.
    ptr_t = typesystem.Type(element_type, '*', name='ptr')
    size_t = typesystem.Type.fromstring('size_t sz')
    buffer_type = typesystem_buffer_type(
        ptr_t,
        size_t,
        *extra_members
    )

    # Converting buffer type to numba type results BufferType instance
    # that simplifies checking types when overloading generic methods
    # like len, getitem, setitem, etc.
    buffer_type._params['numba.Type'] = BufferType

    # Create alias to buffer type
    buffer_type._params['typename'] = typename

    # In omniscidb, boolean values are stored as int8 because
    # boolean has three states: false, true, and null.
    numba_type = buffer_type.tonumba(bool_is_int8=True)

    buffer_type._params['tonumba'] = numba_type
    buffer_type._params['pointer'] = make_buffer_ptr_type(buffer_type,
                                                          buffer_ptr_cls)

    return buffer_type
