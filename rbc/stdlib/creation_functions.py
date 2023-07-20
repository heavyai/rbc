"""
Array API specification for creation functions.

https://data-apis.org/array-api/latest/API_specification/creation_functions.html
"""

from rbc import typesystem, errors
from rbc.heavydb import Array, ArrayPointer
from rbc.stdlib import Expose, API
from numba.core import extending, types, cgutils
from numba import TypingError
from numba.core.typing import asnumbatype

__all__ = [
    'full', 'full_like', 'empty_like', 'empty', 'zeros', 'zeros_like',
    'ones', 'ones_like', 'cumsum', 'arange', 'asarray',
    'eye', 'from_dlpack', 'linspace', 'meshgrid', 'tril', 'triu'
]


expose = Expose(globals(), 'creation_functions')


def _determine_dtype(dtype, fill_value):
    if dtype is None:
        # determine from fill_value
        if fill_value is None:
            # return the default floating-point data type
            return types.double
        else:
            return asnumbatype.typeof(fill_value)
    else:
        if isinstance(dtype, types.UnicodeType):
            raise errors.RequireLiteralValue(dtype)
        elif isinstance(dtype, types.StringLiteral):
            return typesystem.Type.fromstring(dtype.literal_value).tonumba()
        else:
            if not isinstance(dtype, types.DTypeSpec):
                raise TypingError('Expected dtype derived from numba.types.DTypeSpec '
                                  f'but got {type(dtype)}')
            return typesystem.Type.fromobject(dtype).tonumba()


@expose.implements('arange')
def _array_api_arange(start, stop=None, step=1, dtype=None, device=None):
    """
    Return evenly spaced values within a given interval.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('int64[](int64)')
    ... def rbc_arange(start):
    ...     return array_api.arange(start)
    >>> rbc_arange(5).execute()
    array([0, 1, 2, 3, 4])

    >>> @heavydb('double[](int64, float, float)')
    ... def rbc_arange3(start, stop, step):
    ...     return array_api.arange(start, stop, step)
    >>> rbc_arange3(0, 1.0, 0.2).execute()
    array([0. , 0.2, 0.4, 0.6, 0.8], dtype=float32)

    >>> @heavydb('int8[](int64, int64)')
    ... def rbc_arange3(start, stop):
    ...     return array_api.arange(start, stop, dtype=array_api.int8)
    >>> rbc_arange3(2, 5).execute()
    array([2, 3, 4], dtype=int8)

    """

    # Code based on Numba implementation of "np.arange(...)"

    def _arange_dtype(*args):
        """
        If dtype is None, the output array data type must be inferred from
        start, stop and step. If those are all integers, the output array dtype
        must be the default integer dtype; if one or more have type float, then
        the output array dtype must be the default real-valued floating-point
        data type.
        """
        if any(isinstance(a, types.Float) for a in args):
            return types.float64
        else:
            return types.int64

    if not isinstance(start, types.Number):
        raise errors.NumbaTypeError('"start" must be a number')

    if not cgutils.is_nonelike(stop) and not isinstance(stop, types.Number):
        raise errors.NumbaTypeError('"stop" must be a number')

    if cgutils.is_nonelike(dtype):
        true_dtype = _arange_dtype(start, stop, step)
    elif isinstance(dtype, types.DTypeSpec):
        true_dtype = dtype.dtype
    elif isinstance(dtype, types.StringLiteral):
        true_dtype = dtype.literal_value
    else:
        msg = f'If specified, "dtype" must be a DTypeSpec. Got {dtype}'
        raise errors.NumbaTypeError(msg)

    from rbc.stdlib import array_api

    def impl(start, stop=None, step=1, dtype=None, device=None):
        if stop is None:
            _start, _stop = 0, start
        else:
            _start, _stop = start, stop

        _step = step

        if _step == 0:
            raise ValueError("Maximum allowed size exceeded")

        nitems_c = (_stop - _start) / _step
        nitems_r = int(array_api.ceil(nitems_c))

        nitems = max(nitems_r, 0)
        arr = Array(nitems, true_dtype)
        if nitems == 0:
            arr.set_null()
            return arr
        val = _start
        for i in range(nitems):
            arr[i] = val + (i * _step)
        return arr
    return impl


@expose.implements('asarray')
def _array_api_asarray(obj, dtype=None, device=None, copy=None):
    """
    Convert the input to an array.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('float[](int64[])')
    ... def rbc_asarray(arr):
    ...     return array_api.asarray(arr, dtype=array_api.float32)
    >>> rbc_asarray([1, 2, 3]).execute()
    array([1., 2., 3.], dtype=float32)

    """
    if isinstance(obj, (ArrayPointer, types.List)):
        if isinstance(obj, ArrayPointer):
            nb_dtype = obj.eltype if dtype is None else dtype.dtype
        else:
            nb_dtype = obj.dtype if dtype is None else dtype.dtype

        def impl(obj, dtype=None, device=None, copy=None):
            sz = len(obj)
            arr = Array(sz, nb_dtype)
            for i in range(sz):
                arr[i] = obj[i]
            return arr
        return impl


@expose.not_implemented('eye')
def _array_api_eye(n_rows, n_cols=None, k=0, dtype=None, device=None):
    """
    Return a 2-D array with ones on the diagonal and zeros elsewhere.
    """
    pass


@expose.not_implemented('from_dlpack')
def _array_api_from_dlpack(x):
    """
    """
    pass


@expose.not_implemented('linspace')
def _array_api_linspace(start, stop, num, dtype=None, device=None, endpoint=True):
    """
    Return evenly spaced numbers over a specified interval.
    """
    pass


@expose.not_implemented('meshgrid')
def _array_api_meshgrid(*arrays, indexing='xy'):
    """
    Return coordinate matrices from coordinate vectors.
    """
    pass


@expose.not_implemented('tril')
def _array_api_tril(x, k=0):
    """
    Lower triangle of an array.
    """
    pass


@expose.not_implemented('triu')
def _array_api_triu(x, k=0):
    """
    Upper triangle of an array.
    """
    pass


@expose.implements('full')
def _impl__full(shape, fill_value, dtype=None):
    """
    Return a new array of given shape and type, filled with fill_value.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double[](int64, double)')
    ... def rbc_full(size, fill_value):
    ...     return array_api.full(size, fill_value)
    >>> rbc_full(3, 3.14).execute()
    array([3.14, 3.14, 3.14], dtype=float32)

    >>> @heavydb('int64[](int64, int64)')
    ... def rbc_full2(size, fill_value):
    ...     return array_api.full(size, fill_value, 'int64')
    >>> rbc_full2(2, 0).execute()
    array([0, 0])
    """
    nb_dtype = _determine_dtype(dtype, fill_value)

    def impl(shape, fill_value, dtype=None):
        a = Array(shape, nb_dtype)
        a.fill(nb_dtype(fill_value))
        return a
    return impl


@expose.implements('full_like')
def _impl_full_like(a, fill_value, dtype=None):
    """
    Return a full array with the same shape and type as a given array.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double[](double[], double)')
    ... def full_like(other, fill_value):
    ...     return array_api.full_like(other, fill_value)
    >>> full_like([1.0, 2.0, 3.0], 3.14).execute()
    array([3.14, 3.14, 3.14], dtype=float32)

    >>> @heavydb('int64[](int64[], int64)')
    ... def full_like(other, fill_value):
    ...     return array_api.full_like(other, fill_value, 'int64')
    >>> full_like([1, 2, 3], 3).execute()
    array([3, 3, 3])
    """
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = _determine_dtype(dtype, fill_value)

        def impl(a, fill_value, dtype=None):
            sz = len(a)
            other = Array(sz, nb_dtype)
            other.fill(nb_dtype(fill_value))
            return other
        return impl


@expose.implements('empty_like')
def _impl_empty_like(a, dtype=None):
    """
    Return a new array with the same shape and type as a given array.
    """
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = _determine_dtype(dtype, fill_value=None)

        def impl(a, dtype=None):
            return empty(len(a), nb_dtype)  # noqa: F821
        return impl


@expose.implements('empty')
def _impl_empty(shape, dtype=None):
    """
    Return a new array of given shape and type, without initializing entries.
    """
    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = _determine_dtype(dtype, fill_value=None)

    def impl(shape, dtype=None):
        arr = Array(shape, nb_dtype)
        for i in range(shape):
            arr.set_null(i)
        return arr
    return impl


@expose.implements('zeros')
def _impl_zeros(shape, dtype=None):
    """
    Return a new array of given shape and type, filled with zeros.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double[](int64)')
    ... def zeros(size):
    ...     return array_api.zeros(size)
    >>> zeros(3).execute()
    array([0., 0., 0.], dtype=float32)

    >>> @heavydb('int64[](int64)')
    ... def zeros2(size):
    ...     return array_api.zeros(size, 'int64')
    >>> zeros2(2).execute()
    array([0, 0])
    """

    nb_dtype = _determine_dtype(dtype, fill_value=None)
    fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@expose.implements('zeros_like')
def _impl_zeros_like(a, dtype=None):
    """
    Return an array of zeros with the same shape and type as a given array.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('int64[](int64[])')
    ... def zeros_like(other):
    ...     return array_api.zeros_like(other)
    >>> zeros_like([1, 2, 3]).execute()
    array([0, 0, 0])

    >>> @heavydb('float[](int64[])')
    ... def zeros_like2(other):
    ...     return array_api.zeros_like(other, 'float32')
    >>> zeros_like2([1, 2, 3]).execute()
    array([0., 0., 0.], dtype=float32)
    """
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = _determine_dtype(dtype, fill_value=None)

        fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@expose.implements('ones')
def _impl_ones(shape, dtype=None):
    """
    Return a new array of given shape and type, filled with ones.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('double[](int64)')
    ... def ones(size):
    ...     return array_api.ones(size)
    >>> ones(3).execute()
    array([1., 1., 1.], dtype=float32)

    >>> @heavydb('int64[](int64)')
    ... def ones2(size):
    ...     return array_api.ones(size, 'int64')
    >>> ones2(2).execute()
    array([1, 1])
    """

    nb_dtype = _determine_dtype(dtype, fill_value=None)
    fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@expose.implements('ones_like')
def _impl_ones_like(a, dtype=None):
    """
    Return an array of ones with the same shape and type as a given array.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('int64[](int64[])')
    ... def ones_like(other):
    ...     return array_api.ones_like(other)
    >>> ones_like([1, 2, 3]).execute()
    array([1, 1, 1])

    >>> @heavydb('float[](int64[])')
    ... def ones_like2(other):
    ...     return array_api.ones_like(other, 'float32')
    >>> ones_like2([1, 2, 3]).execute()
    array([1., 1., 1.], dtype=float32)
    """
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = _determine_dtype(dtype, fill_value=None)

        fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@extending.overload_method(ArrayPointer, 'fill')
def _impl_array_fill(x, v):
    """
    Fill the array with a scalar value.
    """
    if isinstance(x, ArrayPointer):
        def impl(x, v):
            for i in range(len(x)):
                x[i] = v
        return impl


@expose.implements('cumsum', api=API.NUMPY_API)
def _impl_cumsum(a):
    """
    Return the cumulative sum of the elements along a given axis.
    """
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
