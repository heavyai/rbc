"""
Array API specification for searching functions.

https://data-apis.org/array-api/latest/API_specification/searching_functions.html
"""
from rbc.stdlib import Expose

__all__ = ["argmax", "argmin", "nonzero", "where"]

expose = Expose(globals(), "searching_functions")


@expose.not_implemented("argmax")
def _array_api_argmax(x, *, axis=None, keepdims=False):
    """
    Returns the indices of the maximum values along a specified axis.
    """
    pass


@expose.not_implemented("argmin")
def _array_api_argmin(x, *, axis=None, keepdims=False):
    """
    Returns the indices of the minimum values along a specified axis.
    """
    pass


@expose.not_implemented("nonzero")
def _array_api_nonzero(x):
    """
    Returns the indices of the array elements which are non-zero.
    """
    pass


@expose.not_implemented("where")
def _array_api_where(condition, x1, x2):
    """
    Returns elements chosen from `x1` or `x2` depending on `condition`.
    """
    pass
