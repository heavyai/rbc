"""
Array API specification for set functions.

https://data-apis.org/array-api/latest/API_specification/set_functions.html
"""
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


@expose.not_implemented("unique_values")
def _array_api_unique_values(x):
    """
    Returns the unique elements of an input array `x`.
    """
    pass
