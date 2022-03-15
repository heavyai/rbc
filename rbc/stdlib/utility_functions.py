"""
Array API specification for utility functions.

https://data-apis.org/array-api/latest/API_specification/utility_functions.html
"""
from rbc.stdlib import Expose

__all__ = ["all", "any"]

expose = Expose(globals(), "utility_functions")


@expose.not_implemented("all")
def _array_api_all(x, *, axis=None, keepdims=False):
    """
    Tests whether all input array elements evaluate to `True` along a specified axis.
    """
    pass


@expose.not_implemented("any")
def _array_api_any(x, *, axis=None, keepdims=False):
    """
    Tests whether any input array element evaluates to `True` along a specified axis.
    """
    pass
