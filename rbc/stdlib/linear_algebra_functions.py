"""
Array API specification for linear algebra functions.

https://data-apis.org/array-api/latest/API_specification/linear_algebra_functions.html
"""
from rbc.stdlib import Expose

__all__ = ["matmul", "matrix_transpose", "tensordot", "vecdot"]

expose = Expose(globals(), "linear_algebra_functions")


@expose.not_implemented("matmul")
def _array_api_matmul(x1, x2):
    """
    Computes the matrix product.
    """
    pass


@expose.not_implemented("matrix_transpose")
def _array_api_matrix_transpose(x):
    """
    Transposes a matrix (or a stack of matrices) x.
    """
    pass


@expose.not_implemented("tensordot")
def _array_api_tensordot(x1, x2, *, axes=2):
    """
    Returns a tensor contraction of x1 and x2 over specific axis.
    """
    pass


@expose.not_implemented("vecdot")
def _array_api_vecdot(x1, x2, *, axis=-1):
    """
    Computes the (vector) dot product of two arrays.
    """
    pass
