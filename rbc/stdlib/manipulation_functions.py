"""
Array API specification for manipulation functions.

https://data-apis.org/array-api/latest/API_specification/manipulation_functions.html
"""
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


@expose.not_implemented("concat")
def _array_api_concat(arrays, *, axis=0):
    """
    Joins a sequence of arrays along an existing axis.
    """
    pass


@expose.not_implemented("expand_dims")
def _array_api_expand_dims(x, *, axis=0):
    """
    Expands the shape of an array by inserting a new axis (dimension)
    of size one at the position specified by axis
    """
    pass


@expose.not_implemented("flip")
def _array_api_flip(x, *, axis=None):
    """
    Reverses the order of elements in an array along the given axis.
    """
    pass


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
