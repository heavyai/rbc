"""
Array API specification for sorting functions.

https://data-apis.org/array-api/latest/API_specification/sorting_functions.html
"""
from rbc.stdlib import Expose

__all__ = ["argsort", "sort"]

expose = Expose(globals(), "sorting_functions")


@expose.not_implemented("argsort")
def _array_api_argsort(x, *, axis=-1, descending=False, stable=True):
    """
    Returns the indices that sort an array `x` along a specified axis.
    """
    pass


@expose.not_implemented("sort")
def _array_api_sort(x, *, axis=-1, descending=False, stable=True):
    """
    Returns a sorted copy of an input array `x`.
    """
    pass
