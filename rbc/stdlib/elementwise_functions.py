"""
Array API specification for element-wise functions.

https://data-apis.org/array-api/latest/API_specification/elementwise_functions.html.
"""

from rbc.stdlib import (
    Expose,
    BinaryUfuncExpose,
    UnaryUfuncExpose,
    API,
    determine_input_type,
)
import numpy as np
from rbc import typesystem
from rbc.heavydb import ArrayPointer, Array
from numba.core import types


__all__ = [
    'add', 'subtract', 'multiply', 'divide', 'logaddexp', 'logaddexp2',
    'true_divide', 'floor_divide', 'pow', 'remainder', 'mod',
    'fmod', 'gcd', 'lcm', 'bitwise_and', 'bitwise_or', 'bitwise_xor',
    'bitwise_not', 'atan2', 'hypot',
    'greater', 'greater_equal', 'less', 'less_equal', 'not_equal',
    'equal', 'logical_and', 'logical_or', 'logical_xor', 'maximum',
    'minimum', 'fmax', 'fmin', 'nextafter', 'ldexp', 'negative',
    'positive', 'rint', 'sign', 'abs', 'conj',
    'conjugate', 'exp', 'exp2', 'log', 'log2', 'log10', 'expm1',
    'log1p', 'sqrt', 'square', 'reciprocal', 'bitwise_not', 'sin', 'cos',
    'tan', 'asin', 'acos', 'atan', 'sinh', 'cosh', 'tanh',
    'asinh', 'acosh', 'atanh', 'degrees', 'radians', 'deg2rad',
    'rad2deg', 'logical_not', 'isfinite', 'isinf', 'isnan', 'fabs',
    'floor', 'ceil', 'trunc', 'signbit', 'copysign', 'spacing',
    'heaviside', 'bitwise_left_shift', 'bitwise_right_shift',
    'round', 'isnat',
    # numpy specifics
    'power', 'arctan2', 'left_shift', 'right_shift', 'absolute',
    'invert', 'arcsin', 'arctan', 'arccos', 'arcsinh', 'arccosh', 'arctanh',
    'float_power', 'divmod', 'cbrt',
]


expose = Expose(globals(), "elementwise_functions")
binary_expose = BinaryUfuncExpose(globals(), "elementwise_functions")
unary_expose = UnaryUfuncExpose(globals(), "elementwise_functions")


@unary_expose.implements(np.absolute, func_name="abs")
def _impl_ufunc_abs(x):
    """
    Calculates the absolute value for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.arccos, func_name="acos")
def _impl_ufunc_acos(x):
    """
    Calculates an implementation-dependent approximation of the principal value
    of the inverse cosine for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.arccosh, func_name="acosh")
def _impl_ufunc_acosh(x):
    """
    Calculates an implementation-dependent approximation to the inverse
    hyperbolic cosine for each element x_i of the input array x.
    """
    pass


@binary_expose.implements(np.add, func_name="add")
def _impl_add(x1, x2):
    """
    Calculates the sum for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.arcsin, func_name="asin")
def _impl_ufunc_asin(x):
    """
    Calculates an implementation-dependent approximation of the principal value
    of the inverse sine for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.arcsinh, func_name="asinh")
def _impl_ufunc_asinh(x):
    """
    Calculates an implementation-dependent approximation to the inverse
    hyperbolic sine for each element x_i in the input array x.
    """
    pass


@unary_expose.implements(np.arctan, func_name="atan")
def _impl_ufunc_atan(x):
    """
    Calculates an implementation-dependent approximation of the principal value
    of the inverse tangent for each element x_i of the input array x.
    """
    pass


@binary_expose.implements(np.arctan2, func_name="atan2")
def _impl_ufunc_atan2(x1, x2):
    """
    Calculates an implementation-dependent approximation of the inverse tangent of the quotient
    x1/x2, having domain [-infinity, +infinity] x ``[-infinity, +infinity]`` (where the x notation
    denotes the set of ordered pairs of elements (x1_i, x2_i)) and codomain [-π, +π], for each
    pair of elements (x1_i, x2_i) of the input arrays x1 and x2, respectively.
    """
    pass


@unary_expose.implements(np.arctanh, func_name="atanh")
def _impl_ufunc_atanh(x):
    """
    Calculates an implementation-dependent approximation to the inverse hyperbolic tangent, having
    domain [-1, +1] and codomain [-infinity, +infinity], for each element x_i of the input array
    x.
    """
    pass


@binary_expose.implements(np.bitwise_and, func_name="bitwise_and")
def _impl_ufunc_bitwise_and(x1, x2):
    """
    Computes the bitwise AND of the underlying binary representation of each element x1_iof the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.left_shift, func_name="bitwise_left_shift")
def _impl_ufunc_bitwise_left_shift(x1, x2):
    """
    Shifts the bits of each element x1_i of the input array x1 to the left by appending x2_i
    (i.e., the respective element in the input array x2) zeros to the right of x1_i.
    """
    pass


@unary_expose.implements(np.invert, func_name="bitwise_invert")
def _impl_ufunc_bitwise_invert(x):
    """
    Inverts (flips) each bit for each element x_i of the input array x.
    """
    pass


@binary_expose.implements(np.bitwise_not, func_name="bitwise_not")
def _impl_ufunc_bitwise_not(x1, x2):
    """
    Computes the bitwise NOR of the underlying binary representation of each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.bitwise_or, func_name="bitwise_or")
def _impl_ufunc_bitwise_or(x1, x2):
    """
    Computes the bitwise OR of the underlying binary representation of each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.right_shift, func_name="bitwise_right_shift")
def _impl_ufunc_bitwise_right_shift(x1, x2):
    """
    Shifts the bits of each element x1_i of the input array x1 to the right by appending x2_i
    (i.e., the respective element in the input array x2) zeros to the right of x1_i.
    """
    pass


@binary_expose.implements(np.bitwise_xor, func_name="bitwise_xor")
def _impl_ufunc_bitwise_xor(x1, x2):
    """
    Computes the bitwise XOR of the underlying binary representation of each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.ceil, func_name="ceil", dtype=types.double)
def _impl_ufunc_ceil(x):
    """
    Rounds each element x_i of the input array x to the smallest (i.e., closest to -infinity)
    integer-valued number that is not less than x_i.
    """
    pass


@unary_expose.implements(np.conj, func_name="conj")
def _impl_ufunc_conj(x):
    """
    Returns the complex conjugate for each element x_i of the input array
    """
    pass


@unary_expose.implements(np.cos, func_name="cos")
def _impl_ufunc_cos(x):
    """Calculates an implementation-dependent approximation to the cosine, having domain
    (-infinity, +infinity) and codomain [-1, +1], for each element x_i of the input
    array x.
    """
    pass


@binary_expose.implements(np.divide, func_name="divide")
def _impl_ufunc_divide(x1, x2):
    """
    Calculates the division for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.equal, func_name="equal", dtype=typesystem.boolean8)
def _impl_ufunc_equal(x1, x2):
    """
    Computes the truth value of x1_i == x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.exp, func_name="exp")
def _impl_ufunc_exp(x):
    """Calculates an implementation-dependent approximation to the exponential function, having
    domain.

    [-infinity, +infinity] and codomain [+0, +infinity], for each
    element x_i of the input array x (e raised to the power of x_i,
    where e is the base of the natural logarithm).

    """
    pass


@unary_expose.implements(np.expm1, func_name="expm1")
def _impl_ufunc_expm1(x):
    """Calculates an implementation-dependent approximation to exp(x)-1, having domain [-infinity,

    +infinity] and codomain [-1, +infinity], for each element x_i of the
    input array x.

    """
    pass


@unary_expose.implements(np.floor, func_name="floor", dtype=types.double)
def _impl_ufunc_floor(x):
    """
    Rounds each element x_i of the input array x to the greatest (i.e., closest to +infinity)
    integer-valued number that is not greater than x_i.
    """
    pass


@binary_expose.implements(np.floor_divide, func_name="floor_divide")
def _impl_ufunc_floor_divide(x1, x2):
    """
    Rounds the result of dividing each element x1_i of the input array x1 by the respective
    element x2_i of the input array x2 to the greatest (i.e., closest to +infinity) integer-value
    number that is not greater than the division result.
    """
    pass


@binary_expose.implements(np.greater, func_name="greater", dtype=typesystem.boolean8)
def _impl_ufunc_greater(x1, x2):
    """
    Computes the truth value of x1_i > x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(
    np.greater_equal, func_name="greater_equal", dtype=typesystem.boolean8
)
def _impl_ufunc_greater_equal(x1, x2):
    """
    Computes the truth value of x1_i >= x2_i for each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@unary_expose.not_implemented(func_name="imag")
def _impl_ufunc_imag(x):
    """
    Returns the imaginary component of a complex number for each element x_i of
    the input array x.
    """


@unary_expose.implements(np.isfinite, func_name="isfinite", dtype=typesystem.boolean8)
def _impl_ufunc_isfinite(x):
    """
    Tests each element x_i of the input array x to determine if finite (i.e., not NaN and not
    equal to positive or negative infinity).
    """
    pass


@unary_expose.implements(np.isinf, func_name="isinf", dtype=typesystem.boolean8)
def _impl_ufunc_isinf(x):
    """
    Tests each element x_i of the input array x to determine if equal to positive or negative
    infinity.
    """
    pass


@unary_expose.implements(np.isnan, func_name="isnan", dtype=typesystem.boolean8)
def _impl_ufunc_isnan(x):
    """
    Tests each element x_i of the input array x to determine whether the element is NaN.
    """
    pass


@binary_expose.implements(np.less, func_name="less", dtype=typesystem.boolean8)
def _impl_ufunc_less(x1, x2):
    """
    Computes the truth value of x1_i < x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(
    np.less_equal, func_name="less_equal", dtype=typesystem.boolean8
)
def _impl_ufunc_less_equal(x1, x2):
    """
    Computes the truth value of x1_i <= x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.log, func_name="log")
def _impl_ufunc_log(x):
    """
    Calculates an implementation-dependent approximation to the natural (base e) logarithm, having
    domain [0, +infinity] and codomain [-infinity, +infinity], for each element x_i of the input
    array x.
    """
    pass


@unary_expose.implements(np.log1p, func_name="log1p")
def _impl_ufunc_log1p(x):
    """
    Calculates an implementation-dependent approximation to log(1+x), where log refers to the
    natural (base e) logarithm, having domain [-1, +infinity] and codomain [-infinity, +infinity],
    for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.log2, func_name="log2")
def _impl_ufunc_log2(x):
    """Calculates an implementation-dependent approximation to the base 2 logarithm, having
    domain. [0, +infinity] and codomain [-infinity, +infinity], for each element x_i
    of the input array x.
    """
    pass


@unary_expose.implements(np.log10, func_name="log10")
def _impl_ufunc_log10(x):
    """Calculates an implementation-dependent approximation to the base 10 logarithm, having
    domain. [0, +infinity] and codomain [-infinity, +infinity], for each element x_i
    of the input array x.
    """
    pass


@binary_expose.implements(np.logaddexp, func_name="logaddexp")
def _impl_ufunc_logaddexp(x1, x2):
    """
    Calculates the logarithm of the sum of exponentiations ``log(exp(x1) + exp(x2))`` for each
    element x1_i of the input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(
    np.logical_or, func_name="logical_or", dtype=typesystem.boolean8
)
def _impl_ufunc_logical_or(x1, x2):
    """
    Computes the logical OR for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(
    np.logical_xor, func_name="logical_xor", dtype=typesystem.boolean8
)
def _impl_ufunc_logical_xor(x1, x2):
    """
    "Computes the logical XOR for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.multiply, func_name="multiply")
def _impl_ufunc_multiply(x1, x2):
    """
    Calculates the product for each element x1_i of the input array x1 with the respective element
    x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.negative, func_name="negative")
def _impl_ufunc_negative(x):
    """Computes the numerical negative of each element x_i (i.e., y_i = -x_i) of the
    input array x."""
    pass


@binary_expose.implements(
    np.not_equal, func_name="not_equal", dtype=typesystem.boolean8
)
def _impl_ufunc_not_equal(x1, x2):
    """
    Computes the truth value of x1_i != x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.positive, func_name="positive")
def _impl_ufunc_positive(x):
    """Computes the numerical positive of each element x_i (i.e., y_i = +x_i) of the
    input array x."""
    pass


@binary_expose.implements(np.power, func_name="pow")
def _impl_ufunc_pow(x1, x2):
    """
    Calculates an implementation-dependent approximation of exponentiation by raising each element
    x1_i (the base) of the input array x1 to the power of x2_i (the exponent), where x2_i is the
    corresponding element of the input array x2.
    """
    pass


@unary_expose.not_implemented(func_name="real")
def _impl_ufunc_real(x):
    """
    Returns the real component of a complex number for each element x_i of the
    input array x.
    """
    pass


@binary_expose.implements(np.remainder, func_name="remainder")
def _impl_ufunc_remainder(x1, x2):
    """
    Returns the remainder of division for each element x1_i of the input array x1 and the
    respective element x2_i of the input array x2.
    """
    pass


@unary_expose.implements(np.around, func_name="round")
def _impl_ufunc_round(x):
    """
    Rounds each element x_i of the input array x to the nearest integer-valued number.
    """
    pass


@unary_expose.implements(np.sign, func_name="sign")
def _impl_ufunc_sign(x):
    """
    Returns an indication of the sign of a number for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.sin, func_name="sin")
def _impl_ufunc_sin(x):
    """
    Calculates an implementation-dependent approximation to the sine for each
    element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.sinh, func_name="sinh")
def _impl_ufunc_sinh(x):
    """
    Calculates an implementation-dependent approximation to the hyperbolic sine
    for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.square, func_name="square")
def _impl_ufunc_square(x):
    """
    Squares (x_i * x_i) each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.sqrt, func_name="sqrt")
def _impl_ufunc_sqrt(x):
    """
    Calculates the principal square root for each element x_i of the input
    array x.
    """
    pass


@binary_expose.implements(np.subtract, func_name="subtract")
def _impl_ufunc_subtract(x1, x2):
    """
    Calculates the difference for each element x1_i of the input array x1 with
    the respective element x2_i of the input array x2
    """
    pass


@unary_expose.implements(np.tan, func_name="tan")
def _impl_ufunc_tan(x):
    """
    Calculates an implementation-dependent approximation to the tangent for
    each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.tanh, func_name="tanh")
def _impl_ufunc_tanh(x):
    """
    Calculates an implementation-dependent approximation to the hyperbolic
    tangent for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.trunc, func_name="trunc", dtype=types.double)
def _impl_ufunc_trunc(x):
    """
    Rounds each element x_i of the input array x to the nearest integer-valued
    number that is closer to zero than x_i.
    """
    pass


###############


@binary_expose.implements(np.copysign, api=API.NUMPY_API)
def _impl_ufunc_copysign(x1, x2):
    pass


@binary_expose.implements(np.logaddexp2, api=API.NUMPY_API)
def _impl_ufunc_logaddexp2(x1, x2):
    pass


@binary_expose.implements(np.true_divide, func_name="true_divide", api=API.NUMPY_API)
def _impl_ufunc_true_divide(x1, x2):
    pass


@binary_expose.implements(np.power, func_name="power", api=API.NUMPY_API)
def _impl_ufunc_power(x1, x2):
    pass


@binary_expose.not_implemented("float_power")  # not supported by Numba
def _impl_ufunc_float_power(x1, x2):
    pass


@binary_expose.implements(np.mod, func_name="mod", api=API.NUMPY_API)
def _impl_ufunc_mod(x1, x2):
    pass


@binary_expose.implements(np.fmod, api=API.NUMPY_API)
def _impl_ufunc_fmod(x1, x2):
    pass


@binary_expose.not_implemented("divmod")  # not supported by Numba
def _impl_ufunc_divmod(x1, x2):
    pass


@binary_expose.implements(np.gcd, api=API.NUMPY_API)
def _impl_ufunc_gcd(x1, x2):
    pass


@binary_expose.implements(np.lcm, api=API.NUMPY_API)
def _impl_ufunc_lcm(x1, x2):
    pass


# Bit-twiddling functions
@binary_expose.implements(np.left_shift, api=API.NUMPY_API)
def _impl_ufunc_left_shift(x1, x2):
    pass


@binary_expose.implements(np.right_shift, api=API.NUMPY_API)
def _impl_ufunc_right_shift(x1, x2):
    pass


# trigonometric functions
@binary_expose.implements(np.arctan2, api=API.NUMPY_API)
def _impl_ufunc_arctan2(x1, x2):
    pass


@binary_expose.implements(np.hypot, api=API.NUMPY_API)
def _impl_ufunc_hypot(x1, x2):
    pass


# Comparison functions


@binary_expose.implements(
    np.logical_and, func_name="logical_and", dtype=typesystem.boolean8
)
def _impl_ufunc_logical_and(x1, x2):
    """
    Computes the logical AND for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.maximum, api=API.NUMPY_API)
def _impl_ufunc_maximum(x1, x2):
    pass


@binary_expose.implements(np.minimum, api=API.NUMPY_API)
def _impl_ufunc_minimum(x1, x2):
    pass


@binary_expose.implements(np.fmax, api=API.NUMPY_API)
def _impl_ufunc_fmax(x1, x2):
    pass


@binary_expose.implements(np.fmin, api=API.NUMPY_API)
def _impl_ufunc_fmin(x1, x2):
    pass


# Floating functions
@binary_expose.implements(np.nextafter, api=API.NUMPY_API)
def _impl_ufunc_nextafter(x1, x2):
    pass


@binary_expose.implements(np.ldexp, api=API.NUMPY_API)
def _impl_ufunc_ldexp(x1, x2):
    pass


##################################################################


@unary_expose.implements(np.absolute, api=API.NUMPY_API)
def _impl_ufunc_absolute(x):
    pass


@unary_expose.implements(np.rint, api=API.NUMPY_API)
def _impl_ufunc_rint(x):
    pass


@unary_expose.implements(np.conjugate, api=API.NUMPY_API)
def _impl_ufunc_conjugate(x):
    pass


@unary_expose.implements(np.exp2, api=API.NUMPY_API)
def _impl_ufunc_exp2(x):
    pass


# @unary_expose.implements(np.cbrt)  # not supported by numba
@unary_expose.not_implemented("cbrt")
def _impl_ufunc_cbrt(x):
    pass


@unary_expose.implements(np.reciprocal, api=API.NUMPY_API)
def _impl_ufunc_reciprocal(x):
    pass


# Bit-twiddling functions
@unary_expose.implements(np.invert, api=API.NUMPY_API)
def _impl_ufunc_invert(x):
    pass


# trigonometric functions


@unary_expose.implements(np.arcsin, api=API.NUMPY_API)
def _impl_ufunc_arcsin(x):
    pass


@unary_expose.implements(np.arccos, api=API.NUMPY_API)
def _impl_ufunc_arccos(x):
    pass


@unary_expose.implements(np.arctan, api=API.NUMPY_API)
def _impl_ufunc_arctan(x):
    pass


@unary_expose.implements(np.cosh, func_name="cosh")
def _impl_ufunc_cosh(x):
    """Calculates an implementation-dependent approximation to the hyperbolic cosine, having
    domain.

    [-infinity, +infinity] and codomain [-infinity, +infinity], for each
    element x_i in the input array x.

    """
    pass


@unary_expose.implements(np.arcsinh, api=API.NUMPY_API)
def _impl_ufunc_arcsinh(x):
    pass


@unary_expose.implements(np.arccosh, api=API.NUMPY_API)
def _impl_ufunc_arccosh(x):
    pass


@unary_expose.implements(np.arctanh, api=API.NUMPY_API)
def _impl_ufunc_arctanh(x):
    pass


@unary_expose.implements(np.degrees, api=API.NUMPY_API)
def _impl_ufunc_degrees(x):
    pass


@unary_expose.implements(np.radians, api=API.NUMPY_API)
def _impl_ufunc_radians(x):
    pass


@unary_expose.implements(np.deg2rad, api=API.NUMPY_API)
def _impl_ufunc_deg2rad(x):
    pass


@unary_expose.implements(np.rad2deg, api=API.NUMPY_API)
def _impl_ufunc_rad2deg(x):
    pass


# Comparison functions
@unary_expose.implements(np.logical_not, dtype=typesystem.boolean8)
def _impl_ufunc_logical_not(x):
    """
    Computes the logical NOT for each element x_i of the input array x.
    """
    pass


# Floating functions


@unary_expose.implements(np.fabs, dtype=types.double, api=API.NUMPY_API)
def _impl_ufunc_fabs(x):
    pass


# not supported?
# @unary_expose.implements(np.isnat, dtype=types.int8)
@unary_expose.not_implemented("isnat")
def _impl_ufunc_isnat(x):
    pass


# issue 152:
@unary_expose.implements(np.signbit, dtype=typesystem.boolean8, api=API.NUMPY_API)
def _impl_ufunc_signbit(x):
    pass


@unary_expose.implements(np.spacing, dtype=types.double, api=API.NUMPY_API)
def _impl_ufunc_spacing(x):
    pass


@expose.implements("heaviside", api=API.NUMPY_API)
def _impl_heaviside(x1, x2):
    nb_dtype = types.double
    typA = determine_input_type(x1)
    typB = determine_input_type(x2)
    if isinstance(x1, ArrayPointer):

        def impl(x1, x2):
            sz = len(x1)
            r = Array(sz, nb_dtype)
            for i in range(sz):
                r[i] = heaviside(x1[i], x2)  # noqa: F821
            return r

        return impl
    else:

        def impl(x1, x2):
            if typA(x1) < 0:
                return nb_dtype(0)
            elif typA(x1) == 0:
                return nb_dtype(typB(x2))
            else:
                return nb_dtype(1)

        return impl
