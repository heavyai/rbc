"""
Array API specification for manipulation functions.

https://data-apis.org/array-api/latest/API_specification/manipulation_functions.html
"""
from numba import literal_unroll

from rbc.errors import NumbaTypeError
from rbc.heavydb import ArrayPointer
from rbc.stdlib import Expose

__all__ = [
    "concat",
    "expand_dims",
    "flip",
    "permute_dims",
    "reshape",
    "roll",
    "squeeze",
    "stack",
]

expose = Expose(globals(), "manipulation_functions")


@expose.implements("concat")
def _array_api_concat(*arrays):
    """
    Joins a sequence of arrays along an existing axis.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> import numpy as np
    >>> from rbc.stdlib import array_api
    >>> @heavydb('float32[](int16[], float32[])')
    ... def concat(a, b):
    ...     return array_api.concat(a, b)
    >>> a = np.arange(3, dtype=np.int16)
    >>> b = np.arange(2, dtype=np.float32)
    >>> concat(a, b).execute()
    array([0., 1., 2., 0., 1.], dtype=float32)

    """
    from rbc.stdlib import array_api

    for arr in arrays:
        if not isinstance(arr, ArrayPointer):
            msg = f'Expect array type in "array_api.concat". Got {arr}'
            raise NumbaTypeError(msg)

    def impl(*arrays):
        sz = 0
        for arr in literal_unroll(arrays):
            sz += len(arr)

        dtype = array_api.result_type(*arrays)
        r = array_api.zeros(sz, dtype)

        idx = 0
        for a in literal_unroll(arrays):
            for j in range(len(a)):
                if a.is_null(j):
                    r.set_null(idx)
                else:
                    r[idx] = a[j]
                idx += 1
        return r
    return impl


@expose.not_implemented("expand_dims")
def _array_api_expand_dims(x, *, axis=0):
    """
    Expands the shape of an array by inserting a new axis (dimension)
    of size one at the position specified by axis
    """
    pass


@expose.implements("flip")
def _array_api_flip(x):
    """
    Reverses the order of elements in an array along the given axis.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> import numpy as np
    >>> from rbc.stdlib import array_api
    >>> @heavydb('int16[](int16[])')
    ... def flip(x):
    ...     return array_api.flip(x)
    >>> a = np.arange(4, dtype=np.int16)
    >>> flip(a).execute()
    array([3, 2, 1, 0], dtype=int16)

    >>> b = np.arange(1, dtype=np.int16)
    >>> flip(b).execute()
    array([0], dtype=int16)
    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        def impl(x):
            r = array_api.empty_like(x)
            sz = len(x)
            for i in range(sz):
                if x.is_null(sz-i-1):
                    r.set_null(i)
                else:
                    r[i] = x[sz-i-1]
            return r
        return impl


@expose.not_implemented("permute_dims")
def _array_api_permute_dims(x, axes):
    """
    Permutes the axes (dimensions) of an array x.
    """
    pass


@expose.not_implemented("reshape")
def _array_api_reshape(x, shape, *, copy=None):
    """
    Reshapes an array without changing its data.
    """
    pass


@expose.not_implemented("roll")
def _array_api_roll(x, shift, *, axis=None):
    """
    Rolls array elements along a specified axis.
    """
    pass


@expose.not_implemented("squeeze")
def _array_api_squeeze(x, axis):
    """
    Removes singleton dimensions (axes) from x.
    """
    pass


@expose.not_implemented("stack")
def _array_api_stack(arrays, *, axis=0):
    """
    Joins a sequence of arrays along a new axis.
    """
    pass
