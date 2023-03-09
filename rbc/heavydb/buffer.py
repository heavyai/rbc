"""Implement Buffer type as a base class to HeavyDB Array and Column types.

HeavyDB Buffer represents the following structure:

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
HeavyDB buffer objects from UDF/UDTFs.
"""


import operator
from .metatype import HeavyDBMetaType
from .abstract_type import HeavyDBAbstractType
from llvmlite import ir
import numpy as np
from rbc import typesystem
from rbc.targetinfo import TargetInfo
from rbc.heavydb import extending
from numba.core import datamodel, cgutils, types, imputils

int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()
fp32 = ir.FloatType()
fp64 = ir.DoubleType()


class HeavyDBBufferType(HeavyDBAbstractType):
    """Typesystem type class for HeavyDB buffer structures.
    """
    @property
    def numba_type(self):
        return BufferType

    @property
    def numba_pointer_type(self):
        return BufferPointer

    @classmethod
    def preprocess_args(cls, args):
        assert len(args) == 1, args
        assert len(args[0]) == 1, args
        element_type = args[0][0]
        if not isinstance(element_type, typesystem.Type):
            element_type = typesystem.Type.fromobject(element_type)
        return ((element_type,),)

    @property
    def element_type(self):
        return self[0][0]

    @property
    def buffer_extra_members(self):
        return ()

    @property
    def custom_params(self):
        return {
            'NumbaType': self.numba_type,
            'NumbaPointerType': self.numba_pointer_type,
        }

    def tonumba(self, bool_is_int8=None):
        ptr_t = typesystem.Type(self.element_type, '*', name='ptr')
        size_t = typesystem.Type.fromstring('size_t sz')
        extra_members = tuple(map(typesystem.Type.fromobject, self.buffer_extra_members))
        buffer_type = typesystem.Type(
            ptr_t,
            size_t,
            *extra_members
        )
        buffer_type.params(other=None, **self.custom_params)
        numba_type = buffer_type.tonumba(bool_is_int8=True)
        return self.numba_pointer_type(numba_type)


class BufferType(types.IterableType):
    """Numba type class for HeavyDB buffer structures.
    """

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.members[0].dtype

    @property
    def iterator_type(self):
        return BufferIteratorType(self)


class BufferPointer(types.IterableType):
    """Numba type class for pointers to HeavyDB buffer structures.

    We are not deriving from CPointer because BufferPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        self.eltype = dtype.eltype  # buffer element dtype
        name = "%s[%s]*" % (type(self).__name__, dtype)
        super().__init__(name)

    @property
    def key(self):
        return self.dtype

    @property
    def iterator_type(self):
        return BufferIteratorType(self)

    def deepcopy(self, context, builder, val, retptr):
        raise NotImplementedError


class BufferIteratorType(types.SimpleIteratorType):

    def __init__(self, buffer_type):
        name = f"iter_buffer({buffer_type})"
        self.buffer_type = buffer_type
        super().__init__(name, self.buffer_type.eltype)


@datamodel.register_default(BufferIteratorType)
class BufferPointerIteratorModel(datamodel.StructModel):
    def __init__(self, dmm, fe_type):
        members = [('index', types.EphemeralPointer(types.uintp)),
                   ('buffer', fe_type.buffer_type)]
        super(BufferPointerIteratorModel, self).__init__(dmm, fe_type, members)


class BufferMeta(HeavyDBMetaType):
    pass


class Buffer(object, metaclass=BufferMeta):
    """Represents HeavyDB Buffer that can be constructed within UDF/UDTFs.
    """


@datamodel.register_default(BufferPointer)
class BufferPointerModel(datamodel.models.PointerModel):
    """Base class for HeavyDB buffer pointer models.

    Subclasses should register the model for the corresponding
    BufferPointer subclass, for instance::

      @datamodel.register_default(ArrayPointer)
      class ArrayPointerModel(BufferPointerModel):
          pass
    """


def memalloc(context, builder, ptr_type, element_count, element_size):
    alloc_fn_name = 'allocate_varlen_buffer'
    alloc_fnty = ir.FunctionType(int8_t.as_pointer(), [int64_t, int64_t])
    alloc_fn = cgutils.get_or_insert_function(builder.module,
                                              alloc_fnty,
                                              alloc_fn_name)
    ptr8 = builder.call(alloc_fn, [element_count, element_size])
    ptr = builder.bitcast(ptr8, context.get_value_type(ptr_type))
    return ptr


def heavydb_buffer_constructor(context, builder, sig, args):
    """

    Usage:

      extending.lower_builtin(MyBuffer, numba.types.Integer, ...)(heavydb_buffer_constructor)

    will enable creating MyBuffer instance from a HeavyDB UDF/UDTF definition:

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

    ptr = memalloc(context, builder, ptr_type, element_count, element_size)

    fa = cgutils.create_struct_proxy(sig.return_type.dtype)(context, builder)
    fa.ptr = ptr                  # T*
    fa.sz = element_count         # size_t
    if null_type is not None:
        is_zero = builder.icmp_signed('==', element_count, int64_t(0))
        with builder.if_else(is_zero) as (then, orelse):
            with then:
                is_null = context.get_value_type(null_type)(1)
            with orelse:
                is_null = context.get_value_type(null_type)(0)
        fa.is_null = is_null      # int8_t
    return fa


@extending.intrinsic
def heavydb_buffer_ptr_get_ptr_(typingctx, data):
    eltype = data.eltype
    ptrtype = types.CPointer(eltype)
    sig = ptrtype(data)

    def codegen(context, builder, signature, args):
        data,  = args
        rawptr = cgutils.alloca_once_value(builder, value=data)
        struct = builder.load(builder.gep(rawptr,
                                          [int32_t(0)]))
        return builder.load(builder.gep(struct, [int32_t(0), int32_t(0)]))

    return sig, codegen


@extending.intrinsic
def heavydb_buffer_get_ptr_(typingctx, data):
    eltype = data.eltype
    ptrtype = types.CPointer(eltype)
    sig = ptrtype(data)

    def codegen(context, builder, signature, args):
        data, = args
        assert data.opname == 'load'
        struct = data.operands[0]
        return builder.load(builder.gep(struct, [int32_t(0), int32_t(0)]))

    return sig, codegen


@extending.intrinsic
def heavydb_buffer_ptr_item_get_ptr_(typingctx, data, index):
    eltype = data.eltype
    ptrtype = types.CPointer(eltype)
    sig = ptrtype(data, index)

    def codegen(context, builder, signature, args):
        data, index = args
        rawptr = cgutils.alloca_once_value(builder, value=data)
        struct = builder.load(builder.gep(rawptr, [int32_t(0)]))
        ptr = builder.load(builder.gep(struct, [int32_t(0), int32_t(0)]))
        return builder.gep(ptr, [index])

    return sig, codegen


@extending.overload_method(BufferPointer, 'ptr')
def heavydb_buffer_get_ptr(x, index=None):
    if isinstance(x, BufferPointer):
        if cgutils.is_nonelike(index):
            def impl(x, index=None):
                return heavydb_buffer_ptr_get_ptr_(x)
        else:
            def impl(x, index=None):
                return heavydb_buffer_ptr_item_get_ptr_(x, index)
        return impl
    if isinstance(x, BufferType):
        if cgutils.is_nonelike(index):
            def impl(x, index=None):
                return heavydb_buffer_get_ptr_(x)
        else:
            raise NotImplementedError(f'heavydb_buffer_item_get_ptr_({x}, {index})')
        return impl


@extending.intrinsic
def heavydb_buffer_ptr_len_(typingctx, data):
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
def heavydb_buffer_len_(typingctx, data):
    sig = types.int64(data)

    def codegen(context, builder, signature, args):
        data, = args
        return builder.extract_value(data, [1])

    return sig, codegen


@extending.overload(len)
def heavydb_buffer_len(x):
    if isinstance(x, BufferPointer):
        return lambda x: heavydb_buffer_ptr_len_(x)
    if isinstance(x, BufferType):
        return lambda x: heavydb_buffer_len_(x)


@extending.intrinsic
def heavydb_buffer_ptr_getitem_(typingctx, data, index):
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
def heavydb_buffer_getitem_(typingctx, data, index):
    eltype = data.eltype
    sig = eltype(data, index)

    def codegen(context, builder, signature, args):
        data, index = args
        ptr = builder.extract_value(data, [0])
        res = builder.load(builder.gep(ptr, [index]))

        return res
    return sig, codegen


@extending.overload(operator.getitem)
def heavydb_buffer_getitem(x, i):
    if isinstance(x, BufferPointer):
        return lambda x, i: heavydb_buffer_ptr_getitem_(x, i)
    if isinstance(x, BufferType):
        return lambda x, i: heavydb_buffer_getitem_(x, i)


@extending.intrinsic
def heavydb_buffer_ptr_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    def codegen(context, builder, sig, args):
        zero = int32_t(0)

        data, index, value = args

        rawptr = cgutils.alloca_once_value(builder, value=data)
        ptr = builder.load(rawptr)

        buf = builder.load(builder.gep(ptr, [zero, zero]))
        # [rbc issue-197] Numba promotes operations like
        # int32(a) + int32(b) to int64
        fromty = sig.args[2]
        toty = sig.args[0].eltype
        value = context.cast(builder, value, fromty, toty)
        builder.store(value, builder.gep(buf, [index]))

    return sig, codegen


@extending.intrinsic
def heavydb_buffer_setitem_(typingctx, data, index, value):
    sig = types.none(data, index, value)

    def codegen(context, builder, sig, args):
        data, index, value = args
        ptr = builder.extract_value(data, [0])

        fromty = sig.args[2]
        toty = sig.args[0].eltype
        value = context.cast(builder, value, fromty, toty)

        builder.store(value, builder.gep(ptr, [index]))

    return sig, codegen


@extending.overload(operator.setitem)
def heavydb_buffer_setitem(a, i, v):
    if isinstance(a, BufferPointer):
        return lambda a, i, v: heavydb_buffer_ptr_setitem_(a, i, v)
    if isinstance(a, BufferType):
        return lambda a, i, v: heavydb_buffer_setitem_(a, i, v)


@extending.intrinsic
def heavydb_buffer_is_null_(typingctx, data):
    sig = types.int8(data)

    def codegen(context, builder, sig, args):
        rawptr = cgutils.alloca_once_value(builder, value=args[0])
        ptr = builder.load(rawptr)
        return builder.load(builder.gep(ptr, [int32_t(0), int32_t(2)]))

    return sig, codegen


@extending.intrinsic
def heavydb_buffer_set_null_(typingctx, data):
    sig = types.none(data)

    def codegen(context, builder, sig, args):
        rawptr = cgutils.alloca_once_value(builder, value=args[0])
        ptr = builder.load(rawptr)
        builder.store(int8_t(1), builder.gep(ptr, [int32_t(0), int32_t(2)]))

    return sig, codegen


@extending.intrinsic
def heavydb_buffer_idx_is_null_(typingctx, col_var, row_idx):
    T = col_var.eltype
    sig = types.boolean(col_var, row_idx)

    def codegen(context, builder, signature, args):
        ptr, index = args
        data = builder.extract_value(builder.load(ptr), [0])
        res = builder.load(builder.gep(data, [index]))

        target_info = TargetInfo()
        null_value = target_info.null_values[str(T)]
        # The server sends numbers as unsigned values rather than signed ones.
        # Thus, 129 should be read as -127 (overflow). See rbc issue #254
        nv = ir.Constant(ir.IntType(T.bitwidth), null_value)

        # importing it here in case in the future someone tries to import
        # buffer on timestamp.py
        from .timestamp import TimestampNumbaType
        if isinstance(signature.args[0].eltype, TimestampNumbaType):
            res = builder.extract_value(res, 0)

        if isinstance(T, types.Float):
            res = builder.bitcast(res, nv.type)

        return builder.icmp_signed('==', res, nv)

    def codegen_column_array(context, builder, sig, args):
        ptr, index = args
        i8p = int8_t.as_pointer()
        fnty = ir.FunctionType(int8_t, [i8p, int64_t])
        isNull = cgutils.get_or_insert_function(builder.module, fnty,
                                                "ColumnArray_isNull")
        flatbuffer = builder.extract_value(builder.load(ptr), [0])
        return builder.call(isNull, [flatbuffer, index])

    if isinstance(T, BufferPointer):
        return sig, codegen_column_array
    else:
        return sig, codegen


# "BufferPointer.is_null" checks if a given array or column is null
# as opposed to "BufferType.is_null" that checks if an index in a
# column is null
@extending.overload_method(BufferPointer, 'is_null')
def heavydb_buffer_is_null(x, row_idx=None):
    if isinstance(x, BufferPointer):
        if cgutils.is_nonelike(row_idx):
            def impl(x, row_idx=None):
                return heavydb_buffer_is_null_(x)
        else:
            def impl(x, row_idx=None):
                return heavydb_buffer_idx_is_null_(x, row_idx)
        return impl


def get_null_value(buffer):
    T = buffer.eltype

    target_info = TargetInfo()
    null_value = target_info.null_values[f'{T}']

    # null_values are stored as uin64 values but when assigning to
    # i.e. Column<double>, they null value need to be bitcasted to
    # double.
    toty = T.key
    conv_table = {
        'Timestamp': 'int64',
        'boolean8': 'int8',
        'boolean1': 'int8',
    }
    toty = conv_table.get(toty, toty)

    # The server sends numbers as unsigned values rather than signed ones.
    # Thus, 129 should be read as -127 (overflow). See rbc issue #254
    bitwidth = T.bitwidth
    null_value = np.dtype(f'uint{bitwidth}').type(null_value).view(toty)
    return null_value


@extending.overload_method(BufferPointer, 'set_null')
def heavydb_buffer_set_null(x, row_idx=None):
    if isinstance(x, BufferPointer):
        if cgutils.is_nonelike(row_idx):
            def impl(x, row_idx=None):
                return heavydb_buffer_set_null_(x)
        else:
            null_value = get_null_value(x)

            def impl(x, row_idx=None):
                x[row_idx] = null_value
            return impl
        return impl


@extending.overload(operator.eq)
def dtype_eq(a, b):
    if isinstance(a, types.DTypeSpec) and isinstance(b, types.DTypeSpec):
        eq = (a == b)

        def impl(a, b):
            return eq
        return impl


@extending.overload_attribute(BufferPointer, 'dtype')
def heavydb_buffer_dtype(x):
    if isinstance(x, BufferPointer):
        dtype = x.eltype

        def impl(x):
            return dtype
        return impl


@extending.lower_builtin('iternext', BufferIteratorType)
@imputils.iternext_impl(imputils.RefType.UNTRACKED)
def iternext_BufferPointer(context, builder, sig, args, result):
    [iterbufty] = sig.args
    [bufiter] = args

    iterval = context.make_helper(builder, iterbufty, value=bufiter)

    buf = iterval.buffer
    idx = builder.load(iterval.index)

    if buf.type.is_pointer:
        col = context.make_helper(builder, iterbufty.buffer_type.dtype,
                                  value=builder.load(buf))
    else:
        col = context.make_helper(builder, iterbufty.buffer_type,
                                  value=buf)
    count = col.sz

    is_valid = builder.icmp_signed('<', idx, count)
    result.set_valid(is_valid)

    with builder.if_then(is_valid):
        getitem_fn = context.typing_context.resolve_value_type(operator.getitem)
        getitem_sig = iterbufty.buffer_type.eltype(iterbufty.buffer_type, types.intp)
        getitem_fn.get_call_type(context.typing_context, getitem_sig.args, {})
        getitem_out = context.get_function(getitem_fn, getitem_sig)(builder, [buf, idx])
        result.yield_(getitem_out)
        nidx = builder.add(idx, context.get_constant(types.intp, 1))
        builder.store(nidx, iterval.index)


@extending.lower_builtin('getiter', BufferType)
@extending.lower_builtin('getiter', BufferPointer)
def getiter_buffer_pointer(context, builder, sig, args):
    [buffer] = args

    iterobj = context.make_helper(builder, sig.return_type)

    # set the index to zero
    zero = context.get_constant(types.uintp, 0)
    indexptr = cgutils.alloca_once_value(builder, zero)

    iterobj.index = indexptr

    # wire in the buffer type data
    iterobj.buffer = buffer

    res = iterobj._getvalue()
    return imputils.impl_ret_new_ref(context, builder, sig.return_type, res)
