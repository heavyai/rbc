"""
Array API specification for element-wise functions.

https://data-apis.org/array- api/latest/API_specification/elementwise_functions.html.
"""

from rbc.stdlib import Expose, BinaryUfuncExpose, UnaryUfuncExpose, API, determine_input_type
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


expose = Expose(globals(), 'elementwise_functions')
binary_expose = BinaryUfuncExpose(globals(), 'elementwise_functions')
unary_expose = UnaryUfuncExpose(globals(), 'elementwise_functions')


# math functions
@binary_expose.implements(np.add)
def _impl_add(x1, x2):
    """
    Calculates the sum for each element x1_i of the input array x1 with the respective element
    x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.subtract)
def _impl_ufunc_subtract(x1, x2):
    """
    Calculates the difference for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.multiply)
def _impl_ufunc_multiply(x1, x2):
    """
    Calculates the product for each element x1_i of the input array x1 with the respective element
    x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.divide, ufunc_name='divide')
def _impl_ufunc_divide(x1, x2):
    """
    Calculates the division for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.logaddexp)
def _impl_ufunc_logaddexp(x1, x2):
    """
    Calculates the logarithm of the sum of exponentiations ``log(exp(x1) + exp(x2))`` for each
    element x1_i of the input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.copysign, api=API.NUMPY_API)
def _impl_ufunc_copysign(x1, x2):
    pass


@binary_expose.implements(np.logaddexp2, api=API.NUMPY_API)
def _impl_ufunc_logaddexp2(x1, x2):
    pass


@binary_expose.implements(np.true_divide, api=API.NUMPY_API)
def _impl_ufunc_true_divide(x1, x2):
    pass


@binary_expose.implements(np.floor_divide)
def _impl_ufunc_floor_divide(x1, x2):
    """
    Rounds the result of dividing each element x1_i of the input array x1 by the respective
    element x2_i of the input array x2 to the greatest (i.e., closest to +infinity) integer-value
    number that is not greater than the division result.
    """
    pass


@binary_expose.implements(np.power, ufunc_name='power', api=API.NUMPY_API)
def _impl_ufunc_power(x1, x2):
    pass


@binary_expose.implements(np.power, ufunc_name='pow')
def _impl_ufunc_pow(x1, x2):
    """
    Calculates an implementation-dependent approximation of exponentiation by raising each element
    x1_i (the base) of the input array x1 to the power of x2_i (the exponent), where x2_i is the
    corresponding element of the input array x2.
    """
    pass


@binary_expose.not_implemented('float_power')  # not supported by Numba
def _impl_ufunc_float_power(x1, x2):
    pass


@binary_expose.implements(np.remainder)
def _impl_ufunc_remainder(x1, x2):
    """
    Returns the remainder of division for each element x1_i of the input array x1 and the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.mod, ufunc_name='mod', api=API.NUMPY_API)
def _impl_ufunc_mod(x1, x2):
    pass


@binary_expose.implements(np.fmod, api=API.NUMPY_API)
def _impl_ufunc_fmod(x1, x2):
    pass


@binary_expose.not_implemented('divmod')  # not supported by Numba
def _impl_ufunc_divmod(x1, x2):
    pass


@binary_expose.implements(np.gcd, api=API.NUMPY_API)
def _impl_ufunc_gcd(x1, x2):
    pass


@binary_expose.implements(np.lcm, api=API.NUMPY_API)
def _impl_ufunc_lcm(x1, x2):
    pass


# Bit-twiddling functions
@binary_expose.implements(np.bitwise_and)
def _impl_ufunc_bitwise_and(x1, x2):
    """
    Computes the bitwise AND of the underlying binary representation of each element x1_iof the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.bitwise_or)
def _impl_ufunc_bitwise_or(x1, x2):
    """
    Computes the bitwise OR of the underlying binary representation of each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.bitwise_xor)
def _impl_ufunc_bitwise_xor(x1, x2):
    """
    Computes the bitwise XOR of the underlying binary representation of each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.bitwise_not, ufunc_name='bitwise_not')
def _impl_ufunc_bitwise_not(x1, x2):
    """
    Computes the bitwise NOR of the underlying binary representation of each element x1_i of the
    input array x1 with the respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.left_shift, api=API.NUMPY_API)
def _impl_ufunc_left_shift(x1, x2):
    pass


@binary_expose.implements(np.left_shift, ufunc_name='bitwise_left_shift')
def _impl_ufunc_bitwise_left_shift(x1, x2):
    """
    Shifts the bits of each element x1_i of the input array x1 to the left by appending x2_i
    (i.e., the respective element in the input array x2) zeros to the right of x1_i.
    """
    pass


@binary_expose.implements(np.right_shift, api=API.NUMPY_API)
def _impl_ufunc_right_shift(x1, x2):
    pass


@binary_expose.implements(np.right_shift, ufunc_name='bitwise_right_shift')
def _impl_ufunc_bitwise_right_shift(x1, x2):
    """
    Shifts the bits of each element x1_i of the input array x1 to the right by appending x2_i
    (i.e., the respective element in the input array x2) zeros to the right of x1_i.
    """
    pass


# trigonometric functions
@binary_expose.implements(np.arctan2, api=API.NUMPY_API)
def _impl_ufunc_arctan2(x1, x2):
    pass


@binary_expose.implements(np.arctan2, ufunc_name='atan2')
def _impl_ufunc_atan2(x1, x2):
    """
    Calculates an implementation-dependent approximation of the inverse tangent of the quotient
    x1/x2, having domain [-infinity, +infinity] x ``[-infinity, +infinity]`` (where the x notation
    denotes the set of ordered pairs of elements (x1_i, x2_i)) and codomain [-π, +π], for each
    pair of elements (x1_i, x2_i) of the input arrays x1 and x2, respectively.
    """
    pass


@binary_expose.implements(np.hypot, api=API.NUMPY_API)
def _impl_ufunc_hypot(x1, x2):
    pass


# Comparison functions
@binary_expose.implements(np.greater, dtype=typesystem.boolean8)
def _impl_ufunc_greater(x1, x2):
    """
    Computes the truth value of x1_i > x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.greater_equal, dtype=typesystem.boolean8)
def _impl_ufunc_greater_equal(x1, x2):
    """
    Computes the truth value of x1_i >= x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.less, dtype=typesystem.boolean8)
def _impl_ufunc_less(x1, x2):
    """
    Computes the truth value of x1_i < x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.less_equal, dtype=typesystem.boolean8)
def _impl_ufunc_less_equal(x1, x2):
    """
    Computes the truth value of x1_i <= x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.not_equal, dtype=typesystem.boolean8)
def _impl_ufunc_not_equal(x1, x2):
    """
    Computes the truth value of x1_i != x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.equal, dtype=typesystem.boolean8)
def _impl_ufunc_equal(x1, x2):
    """
    Computes the truth value of x1_i == x2_i for each element x1_i of the input array x1 with the
    respective element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.logical_and, dtype=typesystem.boolean8)
def _impl_ufunc_logical_and(x1, x2):
    """
    Computes the logical AND for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.logical_or, dtype=typesystem.boolean8)
def _impl_ufunc_logical_or(x1, x2):
    """
    Computes the logical OR for each element x1_i of the input array x1 with the respective
    element x2_i of the input array x2.
    """
    pass


@binary_expose.implements(np.logical_xor, dtype=typesystem.boolean8)
def _impl_ufunc_logical_xor(x1, x2):
    """
    "Computes the logical XOR for each element x1_i of the input array x1 with the respective
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


@unary_expose.implements(np.around, ufunc_name='round')
def _impl_ufunc_round(a):
    """
    Rounds each element x_i of the input array x to the nearest integer-valued number.
    """
    pass


@unary_expose.implements(np.negative)
def _impl_ufunc_negative(a):
    """Computes the numerical negative of each element x_i (i.e., y_i = -x_i) of the
    input array x."""
    pass


@unary_expose.implements(np.positive)
def _impl_ufunc_positive(a):
    """Computes the numerical positive of each element x_i (i.e., y_i = +x_i) of the
    input array x."""
    pass


@unary_expose.implements(np.absolute, api=API.NUMPY_API)
def _impl_ufunc_absolute(a):
    pass


@unary_expose.implements(np.absolute, ufunc_name='abs')
def _impl_ufunc_abs(a):
    """
    Calculates the absolute value for each element x_i of the input array x (i.e., the element-
    wise result has the same magnitude as the respective element in x but has positive sign).
    """
    pass


@unary_expose.implements(np.rint, api=API.NUMPY_API)
def _impl_ufunc_rint(a):
    pass


@unary_expose.implements(np.sign)
def _impl_ufunc_sign(a):
    """
    Returns an indication of the sign of a number for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.conj, ufunc_name='conj', api=API.NUMPY_API)
def _impl_ufunc_conj(a):
    pass


@unary_expose.implements(np.conjugate, api=API.NUMPY_API)
def _impl_ufunc_conjugate(a):
    pass


@unary_expose.implements(np.exp)
def _impl_ufunc_exp(a):
    """Calculates an implementation-dependent approximation to the exponential function, having
    domain.

    [-infinity, +infinity] and codomain [+0, +infinity], for each
    element x_i of the input array x (e raised to the power of x_i,
    where e is the base of the natural logarithm).

    """
    pass


@unary_expose.implements(np.exp2, api=API.NUMPY_API)
def _impl_ufunc_exp2(a):
    pass


@unary_expose.implements(np.log)
def _impl_ufunc_log(a):
    """
    Calculates an implementation-dependent approximation to the natural (base e) logarithm, having
    domain [0, +infinity] and codomain [-infinity, +infinity], for each element x_i of the input
    array x.
    """
    pass


@unary_expose.implements(np.log2)
def _impl_ufunc_log2(a):
    """Calculates an implementation-dependent approximation to the base 2 logarithm, having
    domain.

    [0,

    +infinity] and codomain [-infinity, +infinity], for each element x_i
    of the input array x.

    """
    pass


@unary_expose.implements(np.log10)
def _impl_ufunc_log10(a):
    """Calculates an implementation-dependent approximation to the base 10 logarithm, having
    domain.

    [0,

    +infinity] and codomain [-infinity, +infinity], for each element x_i
    of the input array x.

    """
    pass


@unary_expose.implements(np.expm1)
def _impl_ufunc_expm1(a):
    """Calculates an implementation-dependent approximation to exp(x)-1, having domain [-infinity,

    +infinity] and codomain [-1, +infinity], for each element x_i of the
    input array x.

    """
    pass


@unary_expose.implements(np.log1p)
def _impl_ufunc_log1p(a):
    """
    Calculates an implementation-dependent approximation to log(1+x), where log refers to the
    natural (base e) logarithm, having domain [-1, +infinity] and codomain [-infinity, +infinity],
    for each element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.sqrt)
def _impl_ufunc_sqrt(a):
    """
    Calculates the square root, having domain [0, +infinity] and codomain [0, +infinity], for each
    element x_i of the input array x.
    """
    pass


@unary_expose.implements(np.square)
def _impl_ufunc_square(a):
    """Squares (x_i * x_i) each element x_i of the input array x."""
    pass


# @unary_expose.implements(np.cbrt)  # not supported by numba
@unary_expose.not_implemented('cbrt')
def _impl_ufunc_cbrt(a):
    pass


@unary_expose.implements(np.reciprocal, api=API.NUMPY_API)
def _impl_ufunc_reciprocal(a):
    pass


# Bit-twiddling functions
@unary_expose.implements(np.invert, api=API.NUMPY_API)
def _impl_ufunc_invert(a):
    pass


@unary_expose.implements(np.invert, ufunc_name='bitwise_invert')
def _impl_ufunc_bitwise_invert(a):
    """
    Inverts (flips) each bit for each element x_i of the input array x.
    """
    pass


# trigonometric functions
@unary_expose.implements(np.sin)
def _impl_ufunc_sin(a):
    """Calculates an implementation-dependent approximation to the sine, having domain (-infinity,

    +infinity) and codomain [-1, +1], for each element x_i of the input
    array x.

    """
    pass


@unary_expose.implements(np.cos)
def _impl_ufunc_cos(a):
    """Calculates an implementation-dependent approximation to the cosine, having domain
    (-infinity,

    +infinity) and codomain [-1, +1], for each element x_i of the input
    array x.

    """
    pass


@unary_expose.implements(np.tan)
def _impl_ufunc_tan(a):
    """Calculates an implementation-dependent approximation to the tangent, having domain
    (-infinity,

    +infinity) and codomain (-infinity, +infinity), for each element x_i
    of the input array x.

    """
    pass


@unary_expose.implements(np.arcsin, api=API.NUMPY_API)
def _impl_ufunc_arcsin(a):
    pass


@unary_expose.implements(np.arcsin, ufunc_name='asin')
def _impl_ufunc_asin(a):
    """
    Calculates an implementation-dependent approximation of the principal value of the inverse
    sine, having domain [-1, +1] and codomain [-π/2, +π/2] for each element x_i of the input array
    x.
    """
    pass


@unary_expose.implements(np.arccos, api=API.NUMPY_API)
def _impl_ufunc_arccos(a):
    pass


@unary_expose.implements(np.arccos, ufunc_name='acos')
def _impl_ufunc_acos(a):
    """
    Calculates an implementation-dependent approximation of the principal value of the inverse
    cosine, having domain [-1, +1] and codomain [+0, +π], for each element x_i of the input array
    x.
    """
    pass


@unary_expose.implements(np.arctan, api=API.NUMPY_API)
def _impl_ufunc_arctan(a):
    pass


@unary_expose.implements(np.arctan, ufunc_name='atan')
def _impl_ufunc_atan(a):
    """
    Calculates an implementation-dependent approximation of the principal value of the inverse
    tangent, having domain [-infinity, +infinity] and codomain [-π/2, +π/2], for each element x_i
    of the input array x.
    """
    pass


@unary_expose.implements(np.sinh)
def _impl_ufunc_sinh(a):
    """Calculates an implementation-dependent approximation to the hyperbolic sine, having domain.

    [-infinity, +infinity] and codomain [-infinity, +infinity], for each
    element x_i of the input array x.

    """
    pass


@unary_expose.implements(np.cosh)
def _impl_ufunc_cosh(a):
    """Calculates an implementation-dependent approximation to the hyperbolic cosine, having
    domain.

    [-infinity, +infinity] and codomain [-infinity, +infinity], for each
    element x_i in the input array x.

    """
    pass


@unary_expose.implements(np.tanh)
def _impl_ufunc_tanh(a):
    """Calculates an implementation-dependent approximation to the hyperbolic tangent, having
    domain.

    [-infinity, +infinity] and codomain [-1, +1], for each element x_i
    of the input array x.

    """
    pass


@unary_expose.implements(np.arcsinh, api=API.NUMPY_API)
def _impl_ufunc_arcsinh(a):
    pass


@unary_expose.implements(np.arcsinh, ufunc_name='asinh')
def _impl_ufunc_asinh(a):
    """
    Calculates an implementation-dependent approximation to the inverse hyperbolic sine, having
    domain [-infinity, +infinity] and codomain [-infinity, +infinity], for each element x_i in the
    input array x.
    """
    pass


@unary_expose.implements(np.arccosh, api=API.NUMPY_API)
def _impl_ufunc_arccosh(a):
    pass


@unary_expose.implements(np.arccosh, ufunc_name='acosh')
def _impl_ufunc_acosh(a):
    """
    Calculates an implementation-dependent approximation to the inverse hyperbolic cosine, having
    domain [+1, +infinity] and codomain [+0, +infinity], for each element x_i of the input array
    x.
    """
    pass


@unary_expose.implements(np.arctanh, api=API.NUMPY_API)
def _impl_ufunc_arctanh(a):
    pass


@unary_expose.implements(np.arctanh, ufunc_name='atanh')
def _impl_ufunc_atanh(a):
    """
    Calculates an implementation-dependent approximation to the inverse hyperbolic tangent, having
    domain [-1, +1] and codomain [-infinity, +infinity], for each element x_i of the input array
    x.
    """
    pass


@unary_expose.implements(np.degrees, api=API.NUMPY_API)
def _impl_ufunc_degrees(a):
    pass


@unary_expose.implements(np.radians, api=API.NUMPY_API)
def _impl_ufunc_radians(a):
    pass


@unary_expose.implements(np.deg2rad, api=API.NUMPY_API)
def _impl_ufunc_deg2rad(a):
    pass


@unary_expose.implements(np.rad2deg, api=API.NUMPY_API)
def _impl_ufunc_rad2deg(a):
    pass


# Comparison functions
@unary_expose.implements(np.logical_not, dtype=typesystem.boolean8)
def _impl_ufunc_logical_not(a):
    """
    Computes the logical NOT for each element x_i of the input array x.
    """
    pass


# Floating functions
@unary_expose.implements(np.isfinite, dtype=typesystem.boolean8)
def _impl_ufunc_isfinite(a):
    """
    Tests each element x_i of the input array x to determine if finite (i.e., not NaN and not
    equal to positive or negative infinity).
    """
    pass


@unary_expose.implements(np.isinf, dtype=typesystem.boolean8)
def _impl_ufunc_isinf(a):
    """
    Tests each element x_i of the input array x to determine if equal to positive or negative
    infinity.
    """
    pass


@unary_expose.implements(np.isnan, dtype=typesystem.boolean8)
def _impl_ufunc_isnan(a):
    """
    Tests each element x_i of the input array x to determine whether the element is NaN.
    """
    pass


@unary_expose.implements(np.fabs, dtype=types.double, api=API.NUMPY_API)
def _impl_ufunc_fabs(a):
    pass


@unary_expose.implements(np.floor, dtype=types.double)
def _impl_ufunc_floor(a):
    """
    Rounds each element x_i of the input array x to the greatest (i.e., closest to +infinity)
    integer-valued number that is not greater than x_i.
    """
    pass


@unary_expose.implements(np.ceil, dtype=types.double)
def _impl_ufunc_ceil(a):
    """
    Rounds each element x_i of the input array x to the smallest (i.e., closest to -infinity)
    integer-valued number that is not less than x_i.
    """
    pass


@unary_expose.implements(np.trunc, dtype=types.double)
def _impl_ufunc_trunc(a):
    """
    Rounds each element x_i of the input array x to the integer-valued number that is closest to
    but no greater than x_i.
    """
    pass


# not supported?
# @unary_expose.implements(np.isnat, dtype=types.int8)
@unary_expose.not_implemented('isnat')
def _impl_ufunc_isnat(a):
    pass


# issue 152:
@unary_expose.implements(np.signbit, dtype=typesystem.boolean8, api=API.NUMPY_API)
def _impl_ufunc_signbit(a):
    pass


@unary_expose.implements(np.spacing, dtype=types.double, api=API.NUMPY_API)
def _impl_ufunc_spacing(a):
    pass


@expose.implements('heaviside', api=API.NUMPY_API)
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
