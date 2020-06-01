import re
import operator
import warnings
from collections import defaultdict
from llvmlite import ir
import numpy as np
from . import typesystem
from .irtools import printf
from .utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import datamodel, cgutils, extending, types, \
        errors
    from numba.np import numpy_support
else:
    from numba import datamodel, cgutils, extending, types, \
        errors, numpy_support


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()


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


builder_buffers = defaultdict(list)


@extending.lower_builtin(Array, types.Integer, types.StringLiteral)
@extending.lower_builtin(Array, types.Integer, types.NumberClass)
def omnisci_array_constructor(context, builder, sig, args):
    if not context.target_info.is_cpu:
        warnings.warn(
            f'allocating arrays in {context.target_info.name}'
            ' is not supported')
    ptr_type, sz_type, null_type = sig.return_type.dtype.members

    # zero-extend the element count to int64_t
    assert isinstance(args[0].type, ir.IntType), (args[0].type)
    element_count = builder.zext(args[0], int64_t)
    element_size = int64_t(ptr_type.dtype.bitwidth // 8)

    '''
    QueryEngine/ArrayOps.cpp:
    int8_t* allocate_varlen_buffer(int64_t element_count, int64_t element_size)
    '''
    alloc_fnty = ir.FunctionType(int8_t.as_pointer(), [int64_t, int64_t])
    # see https://github.com/xnd-project/rbc/issues/75
    alloc_fn = builder.module.get_or_insert_function(
        alloc_fnty, name="calloc")
    ptr8 = builder.call(alloc_fn, [element_count, element_size])
    builder_buffers[builder].append(ptr8)
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


@extending.overload(np.invert)
def omnisci_np_invert(a):
    """Implements `np.invert(expr)` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a):
            sz = len(a)
            for i in range(sz):
                a[i] = typesystem.boolean8(not a[i])
            return a
        return impl


@extending.overload(operator.eq)
def omnisci_array_eq(a, e):
    """Implements `a == e` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            sz = len(a)
            x = Array(sz, 'int8')
            for i in range(sz):
                x[i] = typesystem.boolean8(a[i] == e)
            return x
        return impl


@extending.overload(operator.ne)
def omnisci_array_ne(a, e):
    """Implements `a != e` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            return np.invert(a == e)
        return impl


@extending.overload(operator.lt)
def omnisci_array_lt(a, e):
    """Implements `a < e` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            sz = len(a)
            x = Array(sz, 'int8')
            for i in range(sz):
                x[i] = typesystem.boolean8(a[i] < e)
            return x
        return impl


@extending.overload(operator.le)
def omnisci_array_le(a, e):
    """Implements `a <= e` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            sz = len(a)
            x = Array(sz, 'int8')
            for i in range(sz):
                x[i] = typesystem.boolean8(a[i] <= e)
            return x
        return impl


@extending.overload(operator.gt)
def omnisci_array_gt(a, e):
    """Implements `a > e` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            return np.invert(a <= e)
        return impl


@extending.overload(operator.ge)
def omnisci_array_ge(a, e):
    """Implements `a >= e` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            return np.invert(a < e)
        return impl


@extending.lower_builtin(operator.is_, ArrayPointer, ArrayPointer)
def _omnisci_array_is(context, builder, sig, args):
    """Implements `a is b` operation
    """
    [a, b] = args
    return builder.icmp_signed('==', a, b)


@extending.lower_builtin(operator.is_not, ArrayPointer, ArrayPointer)
def _omnisci_array_is_not(context, builder, sig, args):
    """Implements `a is not b` operation
    """
    [a, b] = args
    return builder.icmp_signed('!=', a, b)


@extending.overload(operator.contains)
def omnisci_array_contains(a, e):
    """Implements `e in a` operation
    """
    if isinstance(a, ArrayPointer):
        def impl(a, e):
            sz = len(a)
            for i in range(sz):
                if a[i] == e:
                    return True
            return False
        return impl


def get_type_limits(eltype):
    np_dtype = numpy_support.as_dtype(eltype)
    if isinstance(eltype, types.Integer):
        return np.iinfo(np_dtype)
    elif isinstance(eltype, types.Float):
        return np.finfo(np_dtype)
    else:
        msg = 'Type {} not supported'.format(eltype)
        raise errors.TypingError(msg)


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
        min_value = get_type_limits(x.eltype).min

        def impl(x, initial=None):
            if len(x) <= 0:
                printf("omnisci_array_max: cannot find max of zero-sized array")  # noqa: E501
                return min_value
            if initial is not None:
                m = initial
            else:
                m = x[0]
            for i in range(len(x)):
                v = x[i]
                if v > m:
                    m = v
            return m
        return impl


@extending.overload(min)
@extending.overload_method(ArrayPointer, 'min')
def omnisci_array_min(x, initial=None):
    if isinstance(x, ArrayPointer):
        max_value = get_type_limits(x.eltype).max

        def impl(x, initial=None):
            if len(x) <= 0:
                printf("omnisci_array_min: cannot find min of zero-sized array")  # noqa: E501
                return max_value
            if initial is not None:
                m = initial
            else:
                m = x[0]
            for i in range(len(x)):
                v = x[i]
                if v < m:
                    m = v
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
    if isinstance(x.eltype, types.Integer):
        zero_value = 0
    elif isinstance(x.eltype, types.Float):
        zero_value = np.nan

    if isinstance(x, ArrayPointer):
        def impl(x):
            if len(x) == 0:
                printf("Mean of empty array")
                return zero_value
            return sum(x) / len(x)
        return impl


@extending.overload(np.cumsum)
def omnisci_np_cumsum(a):
    if isinstance(a, ArrayPointer):
        eltype = a.eltype

        def impl(a):
            sz = len(a)
            out = Array(sz, eltype)
            out[0] = a[0]
            for i in range(1, sz):
                out[i] = out[i-1] + a[i]
            return out
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
