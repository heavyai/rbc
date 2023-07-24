"""
Array API specification for searching functions.

https://data-apis.org/array-api/latest/API_specification/searching_functions.html
"""
from rbc.stdlib import Expose
from rbc.heavydb import Array, ArrayPointer

__all__ = ["argmax", "argmin", "nonzero", "where"]

expose = Expose(globals(), "searching_functions")


@expose.implements("argmax")
def _array_api_argmax(x, *, axis=None, keepdims=False):
    """
    Returns the indices of the maximum values along a specified axis.
    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        def impl(x, axis=None, keepdims=False):
            argmax = array_api.max(x)
            lst = []
            for i in range(len(x)):
                if array_api.equal(x[i], argmax):
                    lst.append(i)
            return Array(lst)
        return impl


@expose.implements("argmin")
def _array_api_argmin(x, *, axis=None, keepdims=False):
    """
    Returns the indices of the minimum values along a specified axis.
    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        def impl(x, axis=None, keepdims=False):
            argmin = array_api.min(x)
            lst = []
            for i in range(len(x)):
                if array_api.equal(x[i], argmin):
                    lst.append(i)
            return Array(lst)
        return impl


@expose.implements("nonzero")
def _array_api_nonzero(x):
    """
    Returns the indices of the array elements which are non-zero.
    """
    if isinstance(x, ArrayPointer):
        def impl(x):
            lst = []
            for i in range(len(x)):
                if not x.is_null(i) and x[i] != x.dtype(0):
                    lst.append(i)
            return Array(lst)
        return impl


@expose.not_implemented("where")
def _array_api_where(condition, x1, x2):
    """
    Returns elements chosen from `x1` or `x2` depending on `condition`.
    """
    pass
