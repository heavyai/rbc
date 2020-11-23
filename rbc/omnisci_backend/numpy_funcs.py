import numpy as np
from rbc.irtools import printf
from rbc import typesystem
from .omnisci_array import Array, ArrayPointer
from rbc.utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import extending, types, \
        errors
    from numba.np import numpy_support
else:
    from numba import extending, types, \
        errors, numpy_support


def expose_and_overload(func):
    name = func.__name__
    s = f'def {name}(*args, **kwargs): pass'
    exec(s, globals())

    fn = globals()[name]
    decorate = extending.overload(fn)

    def wrapper(overload_func):
        return decorate(overload_func)

    return wrapper


@expose_and_overload(np.full)
def omnisci_np_full(shape, fill_value, dtype=None):

    # XXX: dtype should be infered from fill_value
    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    def impl(shape, fill_value, dtype=None):
        a = Array(shape, nb_dtype)
        a.fill(nb_dtype(fill_value))
        return a
    return impl


@expose_and_overload(np.full_like)
def omnisci_np_full_like(a, fill_value, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        def impl(a, fill_value, dtype=None):
            sz = len(a)
            other = Array(sz, nb_dtype)
            other.fill(nb_dtype(fill_value))
            return other
        return impl


@expose_and_overload(np.empty)
def omnisci_np_empty(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    def impl(shape, dtype=None):
        return Array(shape, nb_dtype)
    return impl


@expose_and_overload(np.zeros)
def omnisci_np_zeros(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@expose_and_overload(np.zeros_like)
def omnisci_np_zeros_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@expose_and_overload(np.ones)
def omnisci_np_ones(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@expose_and_overload(np.ones_like)
def omnisci_np_ones_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = dtype

        fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
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
@expose_and_overload(np.max)
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
@expose_and_overload(np.sum)
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


@expose_and_overload(np.prod)
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


@expose_and_overload(np.mean)
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


@expose_and_overload(np.cumsum)
def omnisci_np_cumsum(a):
    if isinstance(a, ArrayPointer):
        eltype = a.eltype

        def impl(a):
            sz = len(a)
            out = Array(sz, eltype)
            out[0] = a[0]
            for i in range(sz):
                out[i] = out[i-1] + a[i]
            return out
        return impl
