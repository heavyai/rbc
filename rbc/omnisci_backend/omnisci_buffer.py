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


class BufferPointer(types.Type):
    """Type class for pointers to Omnisci buffer structures.

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
    pass


class BufferPointerModel(datamodel.models.PointerModel):
    pass


@extending.intrinsic
def omnisci_buffer_len_(typingctx, data):
    sig = types.int64(data)

    def codegen(context, builder, signature, args):
        data, = args
        rawptr = cgutils.alloca_once_value(builder, value=data)
        struct = builder.load(builder.gep(rawptr,
                                          [int32_t(0)]))
        return builder.load(builder.gep(
            struct, [int32_t(0), int32_t(1)]))
    return sig, codegen


@extending.overload(len)
def omnisci_buffer_len(x):
    if isinstance(x, BufferPointer):
        return lambda x: omnisci_buffer_len_(x)


@extending.intrinsic
def omnisci_buffer_getitem_(typingctx, data, index):
    sig = data.eltype(data, index)

    def codegen(context, builder, signature, args):
        data, index = args
        rawptr = cgutils.alloca_once_value(builder, value=data)
        arr = builder.load(builder.gep(rawptr, [int32_t(0)]))
        ptr = builder.load(builder.gep(
            arr, [int32_t(0), int32_t(0)]))
        res = builder.load(builder.gep(ptr, [index]))

        return res
    return sig, codegen


@extending.overload(operator.getitem)
def omnisci_buffer_getitem(x, i):
    if isinstance(x, BufferPointer):
        return lambda x, i: omnisci_buffer_getitem_(x, i)


@extending.intrinsic
def omnisci_buffer_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    def codegen(context, builder, signature, args):
        zero = int32_t(0)

        data, index, value = args

        rawptr = cgutils.alloca_once_value(builder, value=data)
        ptr = builder.load(rawptr)

        arr = builder.load(builder.gep(ptr, [zero, zero]))
        builder.store(value, builder.gep(arr, [index]))

    return sig, codegen


@extending.overload(operator.setitem)
def omnisci_buffer_setitem(a, i, v):
    if isinstance(a, BufferPointer):
        return lambda a, i, v: omnisci_buffer_setitem_(a, i, v)


def buffer_type_converter(target_info, obj, buffer_cls, buffer_ptr_cls,
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
    obj : {str, numba.Type, typesystem.Type}
      Any object that can be converted to typesystem custom Type instance.
    buffer_cls : {Buffer subclass}
    buffer_ptr_cls : {BufferPointer subclass}
    extra_members : {list of typesystem Type instances}
      Additional members of the buffer structure.

    """
    if not isinstance(obj, typesystem.Type):
        return typesystem.Type.fromobject(obj, target_info)

    name, params = obj.get_name_and_parameters()
    if name is None or name != buffer_cls.__name__:
        return
    assert len(params) == 1, params
    t = typesystem.Type.fromstring(params[0], target_info)
    if not t.is_concrete:
        return

    assert issubclass(buffer_cls, Buffer)
    assert issubclass(buffer_ptr_cls, BufferPointer)

    ptr_t = typesystem.Type(t, '*', name='ptr')

    size_t = typesystem.Type.fromstring('size_t sz',
                                        target_info=target_info)
    buffer_type = typesystem.Type(
        ptr_t,
        size_t,
        *extra_members
    )
    buffer_type_ptr = buffer_type.pointer()

    # In omniscidb, boolean values are stored as int8 because
    # boolean has three states: false, true, and null.
    numba_type_ptr = buffer_ptr_cls(
        buffer_type.tonumba(bool_is_int8=True),
        t.tonumba(bool_is_int8=True))

    typename = '%s<%s>' % (buffer_cls.__name__, t.toprototype())
    buffer_type_ptr._params['typename'] = typename
    buffer_type_ptr._params['tonumba'] = numba_type_ptr

    return buffer_type_ptr
