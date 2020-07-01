import operator
from llvmlite import ir
from .omnisci_array import ArrayPointer, Array
from rbc import typesystem
from rbc.utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import extending, types
else:
    from numba import extending, types


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()


def overload_binary_cmp_op(op):

    def omnisci_operator_impl(a, e):
        if isinstance(a, ArrayPointer):
            if isinstance(e, ArrayPointer):
                def impl(a, e):
                    if len(a) != len(e):
                        return False
                    for i in range(len(a)):
                        if not op(a[i], e[i]):
                            return False
                    return True
            elif isinstance(e, types.Number):
                def impl(a, e):
                    sz = len(a)
                    x = Array(sz, 'int8')
                    for i in range(sz):
                        x[i] = typesystem.boolean8(op(a[i], e))
                    return x
            return impl

    decorate = extending.overload(op)

    def wrapper(overload_func):
        return decorate(omnisci_operator_impl)

    return wrapper


@overload_binary_cmp_op(operator.eq)
@overload_binary_cmp_op(operator.ne)
@overload_binary_cmp_op(operator.lt)
@overload_binary_cmp_op(operator.le)
@overload_binary_cmp_op(operator.gt)
@overload_binary_cmp_op(operator.ge)
def omnisci_binary_cmp_operator_fn(a, e):
    pass


def overload_binary_op(op, inplace=False):

    def omnisci_operator_impl(a, b):
        if isinstance(a, ArrayPointer) and isinstance(b, ArrayPointer):
            nb_dtype = a.eltype

            def impl(a, b):
                # XXX: raise exception if len(a) != len(b)
                sz = len(a)

                if inplace:
                    x = a
                else:
                    x = Array(sz, nb_dtype)

                for i in range(sz):
                    x[i] = nb_dtype(op(a[i], b[i]))
                return x
            return impl

    decorate = extending.overload(op)

    def wrapper(overload_func):
        return decorate(omnisci_operator_impl)

    return wrapper


@overload_binary_op(operator.add)
@overload_binary_op(operator.and_)
@overload_binary_op(operator.floordiv)
@overload_binary_op(operator.lshift)
@overload_binary_op(operator.mod)
@overload_binary_op(operator.mul)
@overload_binary_op(operator.or_)
@overload_binary_op(operator.pow)
@overload_binary_op(operator.rshift)
@overload_binary_op(operator.sub)
@overload_binary_op(operator.truediv)
@overload_binary_op(operator.xor)
@overload_binary_op(operator.iadd, inplace=True)
@overload_binary_op(operator.iand, inplace=True)
@overload_binary_op(operator.ifloordiv, inplace=True)
@overload_binary_op(operator.ilshift, inplace=True)
@overload_binary_op(operator.imod, inplace=True)
@overload_binary_op(operator.imul, inplace=True)
@overload_binary_op(operator.ior, inplace=True)
@overload_binary_op(operator.ipow, inplace=True)
@overload_binary_op(operator.irshift, inplace=True)
@overload_binary_op(operator.isub, inplace=True)
@overload_binary_op(operator.itruediv, inplace=True)
@overload_binary_op(operator.ixor, inplace=True)
def omnisci_binary_operator_fn(a, b):
    pass


def overload_unary_op(op):

    def omnisci_operator_impl(a):
        if isinstance(a, ArrayPointer):
            nb_dtype = a.eltype

            def impl(a):
                sz = len(a)
                x = Array(sz, nb_dtype)
                for i in range(sz):
                    x[i] = nb_dtype(op(a[i]))
                return x
            return impl

    decorate = extending.overload(op)

    def wrapper(overload_func):
        return decorate(omnisci_operator_impl)

    return wrapper


@overload_unary_op(abs)
@overload_unary_op(operator.abs)
@overload_unary_op(operator.neg)
@overload_unary_op(operator.pos)
def omnisci_unary_op(a):
    pass


@extending.overload(operator.countOf)
def omnisci_array_countOf(a, b):
    """
    Return the number of occurrences of b in a
    """
    if isinstance(a, ArrayPointer) and a.eltype == b:
        def impl_countOf(a, b):
            sz = len(a)
            cnt = 0
            for i in range(sz):
                if a[i] == b:
                    cnt += 1
            return cnt
        return impl_countOf


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
