"""
Array API specification for statistical functions.

https://data-apis.org/array-api/latest/API_specification/statistical_functions.html
"""
from rbc.externals.stdio import printf
from rbc import typesystem
from rbc.heavydb import ArrayPointer
from rbc.stdlib import Expose
from numba.core import extending, types, errors
from numba.np import numpy_support
import numpy as np


__all__ = [
    'min', 'max', 'mean', 'prod', 'sum', 'std', 'var'
]


expose = Expose(globals(), 'statistical_functions')


def _get_type_limits(eltype):
    np_dtype = numpy_support.as_dtype(eltype)
    if isinstance(eltype, types.Integer):
        return np.iinfo(np_dtype)
    elif isinstance(eltype, types.Float):
        return np.finfo(np_dtype)
    else:
        msg = 'Type {} not supported'.format(eltype)
        raise errors.TypingError(msg)


@extending.overload(max)
@expose.implements('max')
@extending.overload_method(ArrayPointer, 'max')
def _impl_array_max(x):
    """
    Calculates the maximum value of the input array x
    """
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

        def impl(x):
            if len(x) <= 0:
                printf("impl_array_max: cannot find max of zero-sized array")  # noqa: E501
                return min_value
            m = x[0]
            for i in range(len(x)):
                v = x[i]
                if v > m:
                    m = v
            return m
        return impl


@extending.overload(min)
@expose.implements('min')
@extending.overload_method(ArrayPointer, 'min')
def _impl_array_min(x):
    """
    Calculates the minimum value of the input array x.
    """
    if isinstance(x, ArrayPointer):
        max_value = _get_type_limits(x.eltype).max

        def impl(x):
            if len(x) <= 0:
                printf("impl_array_min: cannot find min of zero-sized array")  # noqa: E501
                return max_value
            m = x[0]
            for i in range(len(x)):
                v = x[i]
                if v < m:
                    m = v
            return m
        return impl


@extending.overload(sum)
@expose.implements('sum')
@extending.overload_method(ArrayPointer, 'sum')
def _impl_np_sum(a):
    """
    Calculates the sum of the input array x.
    """
    if isinstance(a, ArrayPointer):
        def impl(a):
            s = 0
            n = len(a)
            for i in range(n):
                s += a[i]
            return s
        return impl


@expose.implements('prod')
@extending.overload_method(ArrayPointer, 'prod')
def _impl_np_prod(a):
    """
    Calculates the product of input array x elements.
    """
    if isinstance(a, ArrayPointer):
        def impl(a):
            s = 1
            n = len(a)
            for i in range(n):
                s *= a[i]
            return s
        return impl


@expose.implements('mean')
@extending.overload_method(ArrayPointer, 'mean')
def _impl_array_mean(x):
    """
    Calculates the arithmetic mean of the input array x.
    """
    zero_value = np.nan

    if isinstance(x, ArrayPointer):
        def impl(x):
            if len(x) == 0:
                printf("Mean of empty array\n")
                return zero_value
            return sum(x) / len(x)
        return impl


@expose.not_implemented('std')
def _impl_array_std(x, axis=None, correction=0.0, keepdims=False):
    """
    Calculates the standard deviation of the input array x.
    """
    pass


@expose.not_implemented('var')
def _impl_array_var(x, axis=None, correction=0.0, keepdims=False):
    """
    Calculates the variance of the input array x.
    """
    pass
