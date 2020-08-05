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
from llvmlite import ir
from rbc import typesystem
from rbc.utils import get_version
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


class BufferPointer(types.Type):
    """Numba type class for pointers to Omnisci buffer structures.

    We are not deriving from CPointer because BufferPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True

    def __init__(self, dtype, eltype):
        self.dtype = dtype    # struct dtype
        self.eltype = eltype  # buffer element dtype
        name = "(%s)*" % dtype
        types.Type.__init__(self, name)

    @property
    def key(self):
        return self.dtype


class Buffer(object):
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
    eltype = data.members[0].dtype
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


@extending.intrinsic
def omnisci_buffer_ptr_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index, value = args

        rawptr = cgutils.alloca_once_value(builder, value=data)
        ptr = builder.load(rawptr)

        buf = builder.load(builder.gep(ptr, [zero, zero]))
        builder.store(value, builder.gep(buf, [index]))

    return sig, codegen


@extending.intrinsic
def omnisci_buffer_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index, value = args

        assert data.opname == 'load'
        buf = data.operands[0]

        ptr = builder.load(builder.gep(
            buf, [zero, zero]))

        builder.store(value, builder.gep(ptr, [index]))

    return sig, codegen


@extending.overload(operator.setitem)
def omnisci_buffer_setitem(a, i, v):
    if isinstance(a, BufferPointer):
        return lambda a, i, v: omnisci_buffer_ptr_setitem_(a, i, v)
    if isinstance(a, BufferType):
        return lambda a, i, v: omnisci_buffer_setitem_(a, i, v)


def make_buffer_ptr_type(buffer_type, buffer_ptr_cls):
    ptr_type = buffer_type.pointer()

    numba_type = buffer_type.tonumba(bool_is_int8=True)
    numba_eltype = buffer_type[0][0].tonumba(bool_is_int8=True)
    numba_type_ptr = buffer_ptr_cls(numba_type, numba_eltype)

    ptr_type._params['tonumba'] = numba_type_ptr
    ptr_type._params['typename'] = buffer_type._params['typename'] + '*'
    return ptr_type


def buffer_type_converter(target_info, obj, typesystem_buffer_type,
                          buffer_typename, buffer_ptr_cls,
                          extra_members=[]):
    """Template function for converting typesystem Type instances to
    Omnisci Buffer Type instance.

    Note
    ----
    It is assumed that the caller of this function is registered to
    TargetInfo. Otherwise, the convertion from a string object will
    not function correctly.

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

    """
    if isinstance(obj, str):
        obj = typesystem.Type(obj,)

    if not isinstance(obj, typesystem.Type):
        raise NotImplementedError(type(obj))

    # A valid obj for buffer type is in the form
    #   typesystem.Type('BufferTypeName<ElementType>', )
    # In all other cases refuse by returning None.

    name, params = obj.get_name_and_parameters()
    if name is None or name != buffer_typename:
        return
    assert len(params) == 1, params
    t = typesystem.Type.fromstring(params[0], target_info)
    if not t.is_concrete:
        return

    assert issubclass(buffer_ptr_cls, BufferPointer)
    assert issubclass(typesystem_buffer_type, OmnisciBufferType)

    # Construct buffer type as a struct with ptr and sz as members.
    ptr_t = typesystem.Type(t, '*', name='ptr')
    size_t = typesystem.Type.fromstring('size_t sz',
                                        target_info=target_info)
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
    typename = '%s<%s>' % (buffer_typename, t.toprototype())
    buffer_type._params['typename'] = typename

    # In omniscidb, boolean values are stored as int8 because
    # boolean has three states: false, true, and null.
    numba_type = buffer_type.tonumba(bool_is_int8=True)

    buffer_type._params['tonumba'] = numba_type
    buffer_type._params['pointer'] = make_buffer_ptr_type(buffer_type,
                                                          buffer_ptr_cls)

    return buffer_type
