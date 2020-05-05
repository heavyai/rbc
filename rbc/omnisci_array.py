import re
import operator
from llvmlite import ir
import numpy as np
from . import typesystem
from .utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import datamodel, cgutils, extending, types
else:
    from numba import datamodel, cgutils, extending, types


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)


class ArrayPointer(types.Type):
    """Type class for pointers to :code:`Omnisci Array<T>` structure.

    We are not deriving from CPointer because ArrayPointer getitem is
    used to access the data stored in Array ptr member.
    """
    mutable = True

    def __init__(self, dtype, eltype):
        self.dtype = dtype    # i.e. STRUCT__lPLBK
        self.eltype = eltype  # i.e. int64. Base type for dtype: Array<int64>
        name = "(%s)*" % dtype
        super(ArrayPointer, self).__init__(name)

    @property
    def key(self):
        return self.dtype


class Array(object):
    pass


@extending.lower_builtin(Array, types.Integer, types.StringLiteral)
@extending.lower_builtin(Array, types.Integer, types.NumberClass)
def omnisci_array_constructor(context, builder, sig, args):
    ptr_type, sz_type, null_type = sig.return_type.dtype.members

    # zero-extend the element count to int64_t
    assert isinstance(args[0].type, ir.IntType), (args[0].type)
    element_count = builder.zext(args[0], int64_t)
    element_size = int64_t(ptr_type.dtype.bitwidth // 8)

    '''
    QueryEngine/ArrayOps.cpp:
    int8_t* allocate_varlen_buffer(int64_t element_count, int64_t element_size)
    '''
    fnty = ir.FunctionType(int8_t.as_pointer(), [int64_t, int64_t])
    fn = builder.module.get_or_insert_function(
        fnty, name="allocate_varlen_buffer")
    ptr8 = builder.call(fn, [element_count, element_size])
    ptr = builder.bitcast(ptr8, context.get_value_type(ptr_type))
    is_null = context.get_value_type(null_type)(0)

    # construct array
    fa = cgutils.create_struct_proxy(sig.return_type.dtype)(context, builder)
    fa.ptr = ptr              # T*
    fa.sz = element_count     # size_t
    fa.is_null = is_null      # int8_t
    return fa._getpointer()


@extending.type_callable(Array)
def type_omnisci_array(context):
    def typer(size, dtype):
        return array_type_converter(context.target_info, dtype).tonumba()
    return typer


@datamodel.register_default(ArrayPointer)
class ArrayPointerModel(datamodel.models.PointerModel):
    pass


@extending.intrinsic
def omnisci_array_is_null_(typingctx, data):
    sig = types.int8(data)

    def codegen(context, builder, signature, args):

        rawptr = cgutils.alloca_once_value(builder, value=args[0])
        ptr = builder.load(rawptr)

        return builder.load(builder.gep(ptr, [int32_t(0), int32_t(2)]))

    return sig, codegen


@extending.overload_method(ArrayPointer, 'is_null')
def omnisci_array_is_null(x):
    if isinstance(x, ArrayPointer):
        def impl(x):
            return omnisci_array_is_null_(x)
        return impl


@extending.intrinsic
def omnisci_array_len_(typingctx, data):
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
def omnisci_array_len(x):
    if isinstance(x, ArrayPointer):
        return lambda x: omnisci_array_len_(x)


@extending.intrinsic
def omnisci_array_getitem_(typingctx, data, index):
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
def omnisci_array_getitem(x, i):
    if isinstance(x, ArrayPointer):
        return lambda x, i: omnisci_array_getitem_(x, i)


@extending.intrinsic
def omnisci_array_setitem_(typingctx, data, index, value):
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
def omnisci_array_setitem(a, i, v):
    if isinstance(a, ArrayPointer):
        return lambda a, i, v: omnisci_array_setitem_(a, i, v)


@extending.intrinsic
def omnisci_array_logical_and_(typingctx, a, b):
    sig = types.boolean8(a, b)

    def codegen(context, builder, signature, args):
        breakpoint()

    return sig, codegen


@extending.overload_method(ArrayPointer, 'fill')
def omnisci_array_fill(x, v):
    if isinstance(x, ArrayPointer):
        def impl(x, v):
            for i in range(len(x)):
                x[i] = v
        return impl


@extending.overload(max)
@extending.overload(np.max)
@extending.overload_method(ArrayPointer, 'max')
def omnisci_array_max(x, initial=None):
    if isinstance(x, ArrayPointer):
        def impl(x, initial=None):
            if initial is not None:
                m = initial
            else:  # XXX: check if len(array) > 0
                m = x[0]
            for i in range(len(x)):
                m = x[i] if x[i] > m else m
            return m
        return impl


@extending.overload(min)
@extending.overload_method(ArrayPointer, 'min')
def omnisci_array_min(x, initial=None):
    if isinstance(x, ArrayPointer):
        def impl(x, initial=None):
            if initial is not None:
                m = initial
            else:  # XXX: check if len(array) > 0
                m = x[0]
            for i in range(len(x)):
                m = x[i] if x[i] < m else m
            return m
        return impl


@extending.overload(sum)
@extending.overload(np.sum)
@extending.overload_method(ArrayPointer, 'sum')
def omnisci_np_sum(a, initial=None):
    if isinstance(a, ArrayPointer):
        def impl(a, initial=None):
            if initial is not None:
                s = initial
            else:
                s = 0
            n = len(a)
            for i in range(n):
                s += a[i]
            return s
        return impl


@extending.overload(np.prod)
@extending.overload_method(ArrayPointer, 'prod')
def omnisci_np_prod(a, initial=None):
    if isinstance(a, ArrayPointer):
        def impl(a, initial=None):
            if initial is not None:
                s = initial
            else:
                s = 1
            n = len(a)
            for i in range(n):
                s *= a[i]
            return s
        return impl


@extending.overload(np.mean)
@extending.overload_method(ArrayPointer, 'mean')
def omnisci_array_mean(x):
    if isinstance(x, ArrayPointer):
        def impl(x):
            return sum(x) / len(x)
        return impl


_array_type_match = re.compile(r'\A(.*)\s*[\[]\s*[\]]\Z').match


def array_type_converter(target_info, obj):
    """Return Type instance corresponding to Omniscidb `Array` type.

    Omniscidb `Array` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Array {
        T* ptr;
        size_t sz;
        bool is_null;
      }

    Parameters
    ----------
    obj : {str, numba.Type}
      If `obj` is a string then it must be in the form `T[]` where `T`
      specifies the Array items type. Otherwise, `obj` can be any
      object that can be converted to a typesystem.Type object.

    """
    if isinstance(obj, types.StringLiteral):
        obj = obj.literal_value + '[]'
    if isinstance(obj, str):
        m = _array_type_match(obj)
        t = typesystem.Type.fromstring(m.group(1), target_info)
    else:
        t = typesystem.Type.fromobject(obj, target_info)

    ptr_t = typesystem.Type(t, '*', name='ptr')
    typename = 'Array<%s>' % (t.toprototype())
    size_t = typesystem.Type.fromstring('size_t sz',
                                        target_info=target_info)
    array_type = typesystem.Type(
        ptr_t,
        size_t,
        typesystem.Type.fromstring('bool is_null',
                                   target_info=target_info),
    )
    array_type_ptr = array_type.pointer()

    # In omniscidb, boolean values are stored as int8 because
    # boolean has three states: false, true, and null.
    numba_type_ptr = ArrayPointer(
        array_type.tonumba(bool_is_int8=True),
        t.tonumba(bool_is_int8=True))

    array_type_ptr._params['typename'] = typename
    array_type_ptr._params['tonumba'] = numba_type_ptr

    return array_type_ptr
