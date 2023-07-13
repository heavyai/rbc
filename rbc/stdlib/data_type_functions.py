"""
Array API specification for data type functions.

https://data-apis.org/array-api/latest/API_specification/data_type_functions.html
"""
from rbc.stdlib import Expose

__all__ = [
    "astype",
    "broadcast_arrays",
    "broadcast_to",
    "can_cast",
    "finfo",
    "iinfo",
    "result_type",
]

expose = Expose(globals(), "data_type_functions")


@expose.not_implemented("astype")
def _array_api_astype(x, dtype, *, copy=True):
    """
    Copies an array to a specified data type irrespective of
    `Type Promotion Rules <https://data-apis.org/array-api/latest/API_specification/type_promotion.html#type-promotion>`_ rules.
    """
    pass


@expose.not_implemented("broadcast_arrays")
def _array_api_broadcast_arrays(*arrays):
    """
    Broadcasts one or more arrays against one another.
    """
    pass


@expose.not_implemented("broadcast_to")
def _array_api_broadcast_to(x, shape):
    """
    Broadcasts an array to a specified shape.
    """
    pass


@expose.not_implemented("can_cast")
def _array_api_can_cast(from_, to):
    """
    Determines if one data type can be cast to another data type according
    `Type Promotion Rules <https://data-apis.org/array-api/latest/API_specification/type_promotion.html#type-promotion>`_ rules.
    """
    pass


@expose.not_implemented("finfo")
def _array_api_finfo(type):
    """
    Machine limits for floating-point data types.
    """
    pass


@expose.not_implemented("iinfo")
def _array_api_iinfo(type):
    """
    Machine limits for integer data types.
    """
    pass


@expose.not_implemented("result_type")
def _array_api_result_type(*arrays_and_dtypes):
    """
    Returns the dtype that results from applying the type promotion
    rules (see `Type Promotion Rules <https://data-apis.org/array-api/latest/API_specification/type_promotion.html#type-promotion>`_)
    to the arguments.
    """
    pass
