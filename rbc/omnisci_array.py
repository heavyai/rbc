import re
import operator
from llvmlite import ir
import numpy as np
from . import typesystem
from .utils import get_version
from .targetinfo import TargetInfo
if get_version('numba') >= (0, 49):
    from numba.core import datamodel, cgutils, extending, types
else:
    from numba import datamodel, cgutils, extending, types


class ArrayPointer(types.Type):
    """Type class for pointers to :code:`Omnisci Array<T>` structure.

    We are not deriving from CPointer because ArrayPointer getitem is
    used to access the data stored in Array ptr member.
    """
    mutable = True

    def __init__(self, dtype, eltype):
        self.dtype = dtype  # i.e. STRUCT__lPLBK
        self.eltype = eltype  # i.e. int64. Base type for dtype: Array<int64>
        name = "(%s)*" % dtype
        super(ArrayPointer, self).__init__(name)

    @property
    def key(self):
        return self.dtype


class Array(object):
    pass


def omnisci_array_constructor(context, builder, sig, args, elsize):
    pyapi = context.get_python_api(builder)

    # integer types used
    i8 = ir.IntType(8)
    i64 = ir.IntType(64)

    # grab args
    sz, _ = args
    elsize_ir = context.get_value_type(elsize.tonumba())  # get the ir type

    # fill 'sz' and 'is_null'
    typ = sig.return_type.dtype
    fa = cgutils.create_struct_proxy(typ)(context, builder)
    fa.sz = builder.zext(sz, i64)  # zero-extend the size to i64
    fa.is_null = i8(0)

    # fill 'ptr' with the return value of 'allocate_varlen_buffer'
    fnty = ir.FunctionType(i8.as_pointer(), [i64, i64])
    fn = pyapi._get_function(fnty, name="allocate_varlen_buffer")
    call = builder.call(fn, [fa.sz, i64(elsize.bits)])
    fa.ptr = builder.bitcast(call, elsize_ir.as_pointer())

    return fa._getpointer()


@extending.lower_builtin(Array, types.Integer, types.StringLiteral)
def omnisci_array_constructor_string_literal(context, builder, sig, args):
    targetinfo = TargetInfo.host()

    dtype = sig.args[1].literal_value
    elsize = typesystem.Type.fromstring(dtype, targetinfo)  # element size

    return omnisci_array_constructor(context, builder, sig, args, elsize)


@extending.lower_builtin(Array, types.Integer, types.NumberClass)
def omnisci_array_constructor_numba_type(context, builder, sig, args):
    targetinfo = TargetInfo.host()

    it = sig.args[1].instance_type
    elsize = typesystem.Type.fromnumba(it, targetinfo)

    return omnisci_array_constructor(context, builder, sig, args, elsize)


@extending.type_callable(Array)
def type_omnisci_array(context):
    def typer(size, dtype):
        targetinfo = TargetInfo.host()

        if isinstance(dtype, types.NumberClass):
            it = dtype.instance_type
            typ = typesystem.Type.fromnumba(it, targetinfo).tostring() + '[]'
        elif isinstance(dtype, types.StringLiteral):
            typ = dtype.literal_value + '[]'

        conv = array_type_converter(targetinfo, typ)
        return conv._params['tonumba']
    return typer


@datamodel.register_default(ArrayPointer)
class ArrayPointerModel(datamodel.models.PointerModel):
    pass


@extending.intrinsic
def omnisci_array_is_null_(typingctx, data):
    sig = types.int8(data)

    def codegen(context, builder, signature, args):
        i32 = ir.IntType(32)
        zero = i32(0)
        two = i32(2)

        data, = args

        rawptr = cgutils.alloca_once_value(builder, value=data)
        ptr = builder.load(rawptr)

        return builder.load(builder.gep(ptr, [zero, two]))

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
                                          [ir.Constant(ir.IntType(32), 0)]))
        return builder.load(builder.gep(
            struct, [ir.Constant(ir.IntType(32), 0),
                     ir.Constant(ir.IntType(32), 1)]))
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
        arr = builder.load(builder.gep(rawptr,
                                       [ir.Constant(ir.IntType(32), 0)]))
        ptr = builder.load(builder.gep(
            arr, [ir.Constant(ir.IntType(32), 0),
                  ir.Constant(ir.IntType(32), 0)]))
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
        zero = ir.Constant(ir.IntType(32), 0)

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


@extending.overload(np.sum)
@extending.overload(sum)
def omnisci_np_sum(a):
    if isinstance(a, ArrayPointer):
        def impl(a):
            s = 0
            n = len(a)
            for i in range(n):
                s += a[i]
            return s
        return impl


@extending.overload(np.prod)
def omnisci_np_prod(a):
    if isinstance(a, ArrayPointer):
        def impl(a):
            s = 1
            n = len(a)
            for i in range(n):
                s *= a[i]
            return s
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
    obj : str
      Specify a string in the form `T[]` where `T` specifies the Array
      items type.
    """
    if isinstance(obj, str):
        m = _array_type_match(obj)
        if m is not None:
            t = typesystem.Type.fromstring(m.group(1), target_info=target_info)
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
