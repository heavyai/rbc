"""
Array API specification for set functions.

https://data-apis.org/array-api/latest/API_specification/set_functions.html
"""
from numba.core import types

from rbc.heavydb import Array, ArrayPointer
from rbc.stdlib import Expose

__all__ = ["unique_all", "unique_counts", "unique_inverse", "unique_values"]

expose = Expose(globals(), "set_functions")


@expose.not_implemented("unique_all")
def _array_api_unique_all(x):
    """
    Returns the unique elements of an input array `x`, the first
    ocurring indices for each unique element in `x`, the indices from
    the set of unique elements that reconstruct `x`, and the corresponding
    counts for each unique element in `x`.
    """
    pass


@expose.not_implemented("unique_counts")
def _array_api_unique_counts(x):
    """
    Returns the unique elements of an input array `x` and the corresponding
    counts for each unique element in `x`.
    """
    pass


@expose.not_implemented("unique_inverse")
def _array_api_unique_inverse(x):
    """
    Returns the unique elements of an input array `x` and the indices
    from the set of unique elements that reconstruct `x`.
    """
    pass


@expose.implements("unique_values")
def _array_api_unique_values(x):
    """
    Returns the unique elements of an input array `x`.
    """
    if isinstance(x, ArrayPointer):
        if isinstance(x.eltype, types.Float):
            def impl(x):
                if len(x) == 0:
                    return Array(0, x.dtype)
                lst = [x[0]]
                for i in range(1, len(x)):
                    for j in range(0, len(lst)):
                        if x[i] == lst[j]:
                            break
                    else:
                        lst.append(x[i])
                r = Array(len(lst), x.dtype)
                for i in range(len(lst)):
                    r[i] = lst[i]
                return r
        else:
            def impl(x):
                s = set(x.to_list())
                sz = len(s)
                r = Array(sz, x.dtype)
                i = 0
                for e in s:
                    r[i] = e
                    i += 1
                return r
        return impl
