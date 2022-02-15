"""
https://data-apis.org/array-api/latest/API_specification/creation_functions.html
"""

import functools
import numpy as np
from rbc.externals.stdio import printf
from rbc import typesystem
from .omnisci_array import Array, ArrayPointer
from numba import njit
from numba.core import extending, types, errors
from numba.np import numpy_support

__all__ = [
    'full', 'full_like', 'empty_like', 'empty', 'zeros', 'zeros_like',
    'ones', 'ones_like', 'array', 'max', 'min', 'sum', 'prod',
    'mean', 'cumsum'
]


ADDRESS = ("https://data-apis.org/array-api/latest/API_specification"
           "/generated/signatures.creation_functions.{0}.html"
           "#signatures.creation_functions.{1}")


def _expose_and_overload(name):
    s = f'def {name}(*args, **kwargs): pass'
    exec(s, globals())

    fn = globals()[name]
    decorate = extending.overload(fn)

    def wrapper(overload_func):
        overload_func.__doc__ = f"`array-api '{name}' doc <{ADDRESS.format(name, name)}>`_"
        functools.update_wrapper(fn, overload_func)
        return decorate(overload_func)

    return wrapper


def _not_implemented(name):
    s = f'def {name}(*args, **kwargs): pass'
    exec(s, globals())

    fn = globals()[name]
    def wraps(func):
        func.__doc__ = "❌ Not implemented"
        functools.update_wrapper(fn, func)
        return func
    return wraps


@_not_implemented('arange')
def _omnisci_arange(start, stop=None, step=1, dtype=None, device=None):
    pass


@_not_implemented('asarray')
def asarray(obj, dtype=None, device=None, copy=None):
    pass


@_not_implemented('eye')
def eye(n_rows, n_cols=None, k=0, dtype=None, device=None):
	pass


@_not_implemented('from_dlpack')
def from_dlpack(x, /):
	pass


@_not_implemented('linspace')
def linspace(start, stop, num, dtype=None, device=None, endpoint=True):
	pass


@_not_implemented('meshgrid')
def meshgrid(*arrays, indexing='xy'):
	pass


@_not_implemented('tril')
def tril(x, k=0):
	pass


@_not_implemented('triu')
def triu(x, k=0):
	pass


@_expose_and_overload('full')
def _omnisci_np_full(shape, fill_value, dtype=None):

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


@_expose_and_overload('full_like')
def _omnisci_np_full_like(a, fill_value, dtype=None):
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


@_expose_and_overload('empty_like')
def _omnisci_np_empty_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        def impl(a, dtype=None):
            other = Array(0, nb_dtype)
            other.set_null()
            return other
        return impl


@_expose_and_overload('empty')
def _omnisci_np_empty(shape, dtype=None):
    """

    """

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    def impl(shape, dtype=None):
        arr = Array(shape, nb_dtype)
        arr.set_null()
        return arr
    return impl


@_expose_and_overload('zeros')
def _omnisci_np_zeros(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@_expose_and_overload('zeros_like')
def _omnisci_np_zeros_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@_expose_and_overload('ones')
def _omnisci_np_ones(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@_expose_and_overload('ones_like')
def _omnisci_np_ones_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = dtype

        fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@_expose_and_overload('array')
def _omnisci_np_array(a, dtype=None):

    @njit
    def _omnisci_array_non_empty_copy(a, nb_dtype):
        """Implement this here rather than inside "impl".
        LLVM DCE pass removes everything if we implement stuff inside "impl"
        """
        other = Array(len(a), nb_dtype)
        for i in range(len(a)):
            other[i] = a[i]
        return other

    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = dtype

        def impl(a, dtype=None):
            if a.is_null():
                return empty_like(a)  # noqa: F821
            else:
                return omnisci_array_non_empty_copy(a, nb_dtype)
        return impl


def _get_type_limits(eltype):
    np_dtype = numpy_support.as_dtype(eltype)
    if isinstance(eltype, types.Integer):
        return np.iinfo(np_dtype)
    elif isinstance(eltype, types.Float):
        return np.finfo(np_dtype)
    else:
        msg = 'Type {} not supported'.format(eltype)
        raise errors.TypingError(msg)


@extending.overload_method(ArrayPointer, 'fill')
def _omnisci_array_fill(x, v):
    if isinstance(x, ArrayPointer):
        def impl(x, v):
            for i in range(len(x)):
                x[i] = v
        return impl


@extending.overload(max)
@_expose_and_overload('max')
@extending.overload_method(ArrayPointer, 'max')
def _omnisci_array_max(x, initial=None):
    if isinstance(x, ArrayPointer):
        # the array api standard says this is implementation specific
        limits = _get_type_limits(x.eltype)
        t = typesystem.Type.fromobject(x.eltype)
        if t.is_float:
            min_value = limits.min
        elif t.is_int or t.is_uint:
            min_value = 0 if t.is_uint else limits.min + 1
        else:
            raise TypeError(f'Unsupported type {t}')

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
@_expose_and_overload('min')
@extending.overload_method(ArrayPointer, 'min')
def _omnisci_array_min(x, initial=None):
    if isinstance(x, ArrayPointer):
        max_value = _get_type_limits(x.eltype).max

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
@_expose_and_overload('sum')
@extending.overload_method(ArrayPointer, 'sum')
def _omnisci_np_sum(a, initial=None):
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


@_expose_and_overload('prod')
@extending.overload_method(ArrayPointer, 'prod')
def _omnisci_np_prod(a, initial=None):
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


@_expose_and_overload('mean')
@extending.overload_method(ArrayPointer, 'mean')
def _omnisci_array_mean(x):
    zero_value = np.nan

    if isinstance(x, ArrayPointer):
        def impl(x):
            if len(x) == 0:
                printf("Mean of empty array")
                return zero_value
            return sum(x) / len(x)
        return impl


@_expose_and_overload('cumsum')
def _omnisci_np_cumsum(a):
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
