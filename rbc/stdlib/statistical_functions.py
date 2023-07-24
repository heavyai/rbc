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

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double(double[])')
    ... def array_api_max(arr):
    ...     return array_api.max(arr)
    >>> array_api_max([1.0, 3.0, 2.0]).execute()
    3.0

    """
    from rbc.stdlib import array_api

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
            m = min_value
            for i in range(len(x)):
                if x.is_null(i):
                    continue
                v = x[i]
                if array_api.greater(v, m):
                    m = v
            return m
        return impl


@extending.overload(min)
@expose.implements('min')
@extending.overload_method(ArrayPointer, 'min')
def _impl_array_min(x):
    """
    Calculates the minimum value of the input array x.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double(double[])')
    ... def array_api_min(arr):
    ...     return array_api.min(arr)
    >>> array_api_min([1.0, 3.0, 2.0]).execute()
    1.0

    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        max_value = _get_type_limits(x.eltype).max

        def impl(x):
            if len(x) <= 0:
                printf("impl_array_min: cannot find min of zero-sized array")  # noqa: E501
                return max_value
            m = max_value
            for i in range(len(x)):
                if x.is_null(i):
                    continue
                v = x[i]
                if array_api.less(v, m):
                    m = v
            return m
        return impl


@extending.overload(sum)
@expose.implements('sum')
@extending.overload_method(ArrayPointer, 'sum')
def _impl_np_sum(a):
    """
    Calculates the sum of the input array x.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double(double[])')
    ... def array_api_sum(arr):
    ...     return array_api.sum(arr)
    >>> array_api_sum([1.0, 3.0, 2.0]).execute()
    6.0

    """
    if isinstance(a, ArrayPointer):
        def impl(a):
            s = 0
            n = len(a)
            for i in range(n):
                if not a.is_null(i):
                    s += a[i]
            return s
        return impl


@expose.implements('prod')
@extending.overload_method(ArrayPointer, 'prod')
def _impl_np_prod(a):
    """
    Calculates the product of input array x elements.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double(double[])')
    ... def array_api_prod(arr):
    ...     return array_api.prod(arr)
    >>> array_api_prod([4.0, 3.0, 2.0]).execute()
    24.0

    """
    if isinstance(a, ArrayPointer):
        def impl(a):
            s = a.dtype(1)
            n = len(a)
            for i in range(n):
                if not a.is_null(i):
                    s *= a[i]
            return s
        return impl


@expose.implements('mean')
@extending.overload_method(ArrayPointer, 'mean')
def _impl_array_mean(x):
    """
    Calculates the arithmetic mean of the input array x.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double(double[])')
    ... def array_api_mean(arr):
    ...     return array_api.mean(arr)
    >>> array_api_mean([1.0, 3.0, 2.0]).execute()
    2.0

    """
    zero_value = np.nan

    if isinstance(x, ArrayPointer):
        def impl(x):
            y = x.drop_null()
            if len(y) == 0:
                printf("Mean of empty array\n")
                return zero_value
            return sum(y) / len(y)
        return impl


@expose.implements('std')
def _impl_array_std(x, axis=None, correction=0.0, keepdims=False):
    """
    Calculates the standard deviation of the input array x.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('float64(int64[])')
    ... def rbc_std(X):
    ...     return array_api.std(X)
    >>> rbc_std([1, 2, 3]).execute()
    0.8164966

    """

    from rbc.stdlib import array_api

    def impl(x, axis=None, correction=0.0, keepdims=False):
        # std(X) = sqrt(var(X))
        var = array_api.var(x)
        std = array_api.sqrt(var)
        return std

    return impl


@expose.implements('var')
def _impl_array_var(x, axis=None, correction=0.0, keepdims=False):
    """
    Calculates the variance of the input array x.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('float64(int64[])')
    ... def rbc_var(X):
    ...     return array_api.var(X)
    >>> rbc_var([1, 2, 3]).execute()
    0.6666667
    """
    from rbc.stdlib import array_api

    def impl(x, axis=None, correction=0.0, keepdims=False):
        # let
        #  + M be len(X)
        #  + mu be mean(X)
        # var(X) = sum( (X - mu)^2 ) / M
        X = x.drop_null()
        M = len(X)
        if M == 0:
            return array_api.nan
        mean = array_api.mean(X)
        A = array_api.subtract(X, mean)
        B = array_api.power(A, 2)
        C = array_api.sum(B)
        D = C / M
        return D

    return impl
