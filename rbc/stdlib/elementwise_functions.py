"""
https://data-apis.org/array-api/latest/API_specification/elementwise_functions.html
"""

from rbc.stdlib import Expose, BinaryUfuncExpose, UnaryUfuncExpose, API
import numpy as np
from rbc import typesystem
from rbc.omnisci_backend import ArrayPointer, Array
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
    'round',
]


expose = Expose(globals(), 'elementwise_functions')
binary_expose = BinaryUfuncExpose(globals(), 'elementwise_functions')
unary_expose = UnaryUfuncExpose(globals(), 'elementwise_functions')


# math functions
@binary_expose.implements(np.add)
def _omnisci_add(a, b):
    """
    """
    pass


@binary_expose.implements(np.subtract)
def _omnisci_ufunc_subtract(a, b):
    """
    """
    pass


@binary_expose.implements(np.multiply)
def _omnisci_ufunc_multiply(a, b):
    """
    """
    pass


@binary_expose.implements(np.divide, ufunc_name='divide')
def _omnisci_ufunc_divide(a, b):
    """
    """
    pass


@binary_expose.implements(np.logaddexp)
def _omnisci_ufunc_logaddexp(a, b):
    """
    """
    pass


@binary_expose.implements(np.logaddexp2, api=API.NUMPY_API)
def _omnisci_ufunc_logaddexp2(a, b):
    """
    """
    pass


@binary_expose.implements(np.true_divide, api=API.NUMPY_API)
def _omnisci_ufunc_true_divide(a, b):
    """
    """
    pass


@binary_expose.implements(np.floor_divide)
def _omnisci_ufunc_floor_divide(a, b):
    """
    """
    pass


@binary_expose.implements(np.power, ufunc_name='pow')
def _omnisci_ufunc_power(a, b):
    """
    """
    pass


@binary_expose.not_implemented('float_power')  # not supported by Numba
def _omnisci_ufunc_float_power(a, b):
    """
    """
    pass


@binary_expose.implements(np.remainder)
def _omnisci_ufunc_remainder(a, b):
    """
    """
    pass


@binary_expose.implements(np.mod, ufunc_name='mod', api=API.NUMPY_API)
def _omnisci_ufunc_mod(a, b):
    """
    """
    pass


@binary_expose.implements(np.fmod, api=API.NUMPY_API)
def _omnisci_ufunc_fmod(a, b):
    """
    """
    pass


@binary_expose.not_implemented('divmod')  # not supported by Numba
def _omnisci_ufunc_divmod(a, b):
    """
    """
    pass


@binary_expose.implements(np.gcd, api=API.NUMPY_API)
def _omnisci_ufunc_gcd(a, b):
    """
    """
    pass


@binary_expose.implements(np.lcm, api=API.NUMPY_API)
def _omnisci_ufunc_lcm(a, b):
    """
    """
    pass


# Bit-twiddling functions
@binary_expose.implements(np.bitwise_and)
def _omnisci_ufunc_bitwise_and(a, b):
    """
    """
    pass


@binary_expose.implements(np.bitwise_or)
def _omnisci_ufunc_bitwise_or(a, b):
    """
    """
    pass


@binary_expose.implements(np.bitwise_xor)
def _omnisci_ufunc_bitwise_xor(a, b):
    """
    """
    pass


@binary_expose.implements(np.bitwise_not, ufunc_name='bitwise_not')
def _omnisci_ufunc_bitwise_not(a, b):
    """
    """
    pass


@binary_expose.implements(np.left_shift, ufunc_name='bitwise_left_shift')
def _omnisci_ufunc_left_shift(a, b):
    """
    """
    pass


@binary_expose.implements(np.right_shift, ufunc_name='bitwise_right_shift')
def _omnisci_ufunc_right_shift(a, b):
    """
    """
    pass


# trigonometric functions
@binary_expose.implements(np.arctan2, ufunc_name='atan2')
def _omnisci_ufunc_arctan2(a, b):
    """
    """
    pass


@binary_expose.implements(np.hypot, api=API.NUMPY_API)
def _omnisci_ufunc_hypot(a, b):
    """
    """
    pass


# Comparison functions
@binary_expose.implements(np.greater, dtype=typesystem.boolean8)
def _omnisci_ufunc_greater(a, b):
    """
    """
    pass


@binary_expose.implements(np.greater_equal, dtype=typesystem.boolean8)
def _omnisci_ufunc_greater_equal(a, b):
    """
    """
    pass


@binary_expose.implements(np.less, dtype=typesystem.boolean8)
def _omnisci_ufunc_less(a, b):
    """
    """
    pass


@binary_expose.implements(np.less_equal, dtype=typesystem.boolean8)
def _omnisci_ufunc_less_equal(a, b):
    """
    """
    pass


@binary_expose.implements(np.not_equal, dtype=typesystem.boolean8)
def _omnisci_ufunc_not_equal(a, b):
    """
    """
    pass


@binary_expose.implements(np.equal, dtype=typesystem.boolean8)
def _omnisci_ufunc_equal(a, b):
    """
    """
    pass


@binary_expose.implements(np.logical_and, dtype=typesystem.boolean8)
def _omnisci_ufunc_logical_and(a, b):
    """
    """
    pass


@binary_expose.implements(np.logical_or, dtype=typesystem.boolean8)
def _omnisci_ufunc_logical_or(a, b):
    """
    """
    pass


@binary_expose.implements(np.logical_xor, dtype=typesystem.boolean8)
def _omnisci_ufunc_logical_xor(a, b):
    """
    """
    pass


@binary_expose.implements(np.maximum, api=API.NUMPY_API)
def _omnisci_ufunc_maximum(a, b):
    """
    """
    pass


@binary_expose.implements(np.minimum, api=API.NUMPY_API)
def _omnisci_ufunc_minimum(a, b):
    """
    """
    pass


@binary_expose.implements(np.fmax, api=API.NUMPY_API)
def _omnisci_ufunc_fmax(a, b):
    """
    """
    pass


@binary_expose.implements(np.fmin, api=API.NUMPY_API)
def _omnisci_ufunc_fmin(a, b):
    """
    """
    pass


# Floating functions
@binary_expose.implements(np.nextafter, api=API.NUMPY_API)
def _omnisci_ufunc_nextafter(a, b):
    """
    """
    pass


@binary_expose.implements(np.ldexp, api=API.NUMPY_API)
def _omnisci_ufunc_ldexp(a, b):
    """
    """
    pass


##################################################################


@unary_expose.implements(np.around, ufunc_name='round')
def _omnisci_unary_round(a):
    """
    """
    pass


@unary_expose.implements(np.negative)
def _omnisci_unary_negative(a):
    """
    """
    pass


@unary_expose.implements(np.positive)
def _omnisci_unary_positive(a):
    """
    """
    pass


@unary_expose.implements(np.absolute, ufunc_name='abs')
def _omnisci_unary_absolute(a):
    """
    """
    pass


@unary_expose.implements(np.rint, api=API.NUMPY_API)
def _omnisci_unary_rint(a):
    """
    """
    pass


@unary_expose.implements(np.sign)
def _omnisci_unary_sign(a):
    """
    """
    pass


@unary_expose.implements(np.conj, ufunc_name='conj', api=API.NUMPY_API)
def _omnisci_unary_conj(a):
    """
    """
    pass


@unary_expose.implements(np.conjugate, api=API.NUMPY_API)
def _omnisci_unary_conjugate(a):
    """
    """
    pass


@unary_expose.implements(np.exp)
def _omnisci_unary_exp(a):
    """
    """
    pass


@unary_expose.implements(np.exp2, api=API.NUMPY_API)
def _omnisci_unary_exp2(a):
    """
    """
    pass


@unary_expose.implements(np.log)
def _omnisci_unary_log(a):
    """
    """
    pass


@unary_expose.implements(np.log2)
def _omnisci_unary_log2(a):
    """
    """
    pass


@unary_expose.implements(np.log10)
def _omnisci_unary_log10(a):
    """
    """
    pass


@unary_expose.implements(np.expm1)
def _omnisci_unary_expm1(a):
    """
    """
    pass


@unary_expose.implements(np.log1p)
def _omnisci_unary_log1p(a):
    """
    """
    pass


@unary_expose.implements(np.sqrt)
def _omnisci_unary_sqrt(a):
    """
    """
    pass


@unary_expose.implements(np.square)
def _omnisci_unary_square(a):
    """
    """
    pass


# @unary_expose.implements(np.cbrt)  # not supported by numba
@unary_expose.not_implemented('cbrt')
def _omnisci_unary_cbrt(a):
    """
    """
    pass


@unary_expose.implements(np.reciprocal, api=API.NUMPY_API)
def _omnisci_unary_reciprocal(a):
    """
    """
    pass


# Bit-twiddling functions
@unary_expose.implements(np.invert, ufunc_name='bitwise_invert')
def _omnisci_unary_invert(a):
    """
    """
    pass


# trigonometric functions
@unary_expose.implements(np.sin)
def _omnisci_unary_sin(a):
    """
    """
    pass


@unary_expose.implements(np.cos)
def _omnisci_unary_cos(a):
    """
    """
    pass


@unary_expose.implements(np.tan)
def _omnisci_unary_tan(a):
    """
    """
    pass


@unary_expose.implements(np.arcsin, ufunc_name='asin')
def _omnisci_unary_arcsin(a):
    """
    """
    pass


@unary_expose.implements(np.arccos, ufunc_name='acos')
def _omnisci_unary_arccos(a):
    """
    """
    pass


@unary_expose.implements(np.arctan, ufunc_name='atan')
def _omnisci_unary_arctan(a):
    """
    """
    pass


@unary_expose.implements(np.sinh)
def _omnisci_unary_sinh(a):
    """
    """
    pass


@unary_expose.implements(np.cosh)
def _omnisci_unary_cosh(a):
    """
    """
    pass


@unary_expose.implements(np.tanh)
def _omnisci_unary_tanh(a):
    """
    """
    pass


@unary_expose.implements(np.arcsinh, ufunc_name='asinh')
def _omnisci_unary_arcsinh(a):
    """
    """
    pass


@unary_expose.implements(np.arccosh, ufunc_name='acosh')
def _omnisci_unary_arccosh(a):
    """
    """
    pass


@unary_expose.implements(np.arctanh, ufunc_name='atanh')
def _omnisci_unary_arctanh(a):
    """
    """
    pass


@unary_expose.implements(np.degrees, api=API.NUMPY_API)
def _omnisci_unary_degrees(a):
    """
    """
    pass


@unary_expose.implements(np.radians, api=API.NUMPY_API)
def _omnisci_unary_radians(a):
    """
    """
    pass


@unary_expose.implements(np.deg2rad, api=API.NUMPY_API)
def _omnisci_unary_deg2rad(a):
    """
    """
    pass


@unary_expose.implements(np.rad2deg, api=API.NUMPY_API)
def _omnisci_unary_rad2deg(a):
    """
    """
    pass


# Comparison functions
@unary_expose.implements(np.logical_not, dtype=typesystem.boolean8)
def _omnisci_unary_logical_not(a):
    """
    """
    pass


# Floating functions
@unary_expose.implements(np.isfinite, dtype=typesystem.boolean8)
def _omnisci_unary_isfinite(a):
    """
    """
    pass


@unary_expose.implements(np.isinf, dtype=typesystem.boolean8)
def _omnisci_unary_isinf(a):
    """
    """
    pass


@unary_expose.implements(np.isnan, dtype=typesystem.boolean8)
def _omnisci_unary_isnan(a):
    """
    """
    pass


@unary_expose.implements(np.fabs, dtype=types.double, api=API.NUMPY_API)
def _omnisci_unary_fabs(a):
    """
    """
    pass


@unary_expose.implements(np.floor, dtype=types.double)
def _omnisci_unary_floor(a):
    """
    """
    pass


@unary_expose.implements(np.ceil, dtype=types.double)
def _omnisci_unary_ceil(a):
    """
    """
    pass


@unary_expose.implements(np.trunc, dtype=types.double)
def _omnisci_unary_trunc(a):
    """
    """
    pass


# not supported?
# @unary_expose.implements(np.isnat, dtype=types.int8)
@unary_expose.not_implemented('isnat')
def _omnisci_unary_isnat(a):
    """
    """
    pass


# issue 152:
@unary_expose.implements(np.signbit, dtype=typesystem.boolean8, api=API.NUMPY_API)
def _omnisci_unary_signbit(a):
    """
    """
    pass


@unary_expose.implements(np.copysign, api=API.NUMPY_API)
def _omnisci_unary_copysign(a):
    """
    """
    pass


@unary_expose.implements(np.spacing, dtype=types.double, api=API.NUMPY_API)
def _omnisci_unary_spacing(a):
    """
    docstring for np.spacing
    """
    pass


@expose.implements('heaviside', api=API.NUMPY_API)
def _impl_heaviside(x1, x2):
    """
    """
    nb_dtype = types.double
    typA = binary_expose.determine_input_type(x1)
    typB = binary_expose.determine_input_type(x2)
    if isinstance(x1, ArrayPointer):
        def impl(x1, x2):
            sz = len(x1)
            r = Array(sz, nb_dtype)
            for i in range(sz):
                r[i] = heaviside(x1[i], x2)
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
