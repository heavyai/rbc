"""
Array API specification for creation functions.

https://data-apis.org/array-api/latest/API_specification/creation_functions.html
"""

from rbc import typesystem, errors
from rbc.heavydb import Array, ArrayPointer
from rbc.stdlib import Expose, API
from numba.core import extending, types
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


@expose.not_implemented('arange')
def _array_api_arange(start, stop=None, step=1, dtype=None, device=None):
    """
    Return evenly spaced values within a given interval.
    """
    pass


@expose.not_implemented('asarray')
def _array_api_asarray(obj, dtype=None, device=None, copy=None):
    """
    Convert the input to an array.
    """
    pass


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
