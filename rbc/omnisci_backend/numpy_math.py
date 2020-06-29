import numpy as np
from .omnisci_array import Array, ArrayPointer
from rbc.utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import extending, types
else:
    from numba import extending, types


def overload_elementwise_binary_ufunc(ufunc, name=None, dtype=None):
    """
    Wrapper for binary ufuncs that returns an array
    """
    if name is None:
        name = ufunc.__name__
    globals()[name] = ufunc

    def binary_ufunc_impl(a, b):
        # XXX: raise error if len(a) != len(b)
        if isinstance(a, ArrayPointer) and isinstance(b, ArrayPointer):
            if dtype is None:
                nb_dtype = a.eltype
            else:
                nb_dtype = dtype

            def impl(a, b):
                sz = len(a)
                x = Array(sz, nb_dtype)
                for i in range(sz):
                    x[i] = nb_dtype(ufunc(a[i], b[i]))
                return x
            return impl

    decorate = extending.overload(ufunc)

    def wrapper(overload_func):
        return decorate(binary_ufunc_impl)

    return wrapper


# math functions
@overload_elementwise_binary_ufunc(np.add)
@overload_elementwise_binary_ufunc(np.subtract)
@overload_elementwise_binary_ufunc(np.multiply)
@overload_elementwise_binary_ufunc(np.divide, name='divide')
@overload_elementwise_binary_ufunc(np.logaddexp)
@overload_elementwise_binary_ufunc(np.logaddexp2)
@overload_elementwise_binary_ufunc(np.true_divide)
@overload_elementwise_binary_ufunc(np.floor_divide)
@overload_elementwise_binary_ufunc(np.power)
@overload_elementwise_binary_ufunc(np.remainder)
@overload_elementwise_binary_ufunc(np.mod, name='mod')
@overload_elementwise_binary_ufunc(np.fmod)
# @overload_elementwise_binary_ufunc(np.divmod) # not supported by numba
@overload_elementwise_binary_ufunc(np.gcd)
@overload_elementwise_binary_ufunc(np.lcm)
# Bit-twiddling functions
@overload_elementwise_binary_ufunc(np.bitwise_and)
@overload_elementwise_binary_ufunc(np.bitwise_or)
@overload_elementwise_binary_ufunc(np.bitwise_xor)
@overload_elementwise_binary_ufunc(np.left_shift)
@overload_elementwise_binary_ufunc(np.right_shift)
# trigonometric functions
@overload_elementwise_binary_ufunc(np.arctan2)
@overload_elementwise_binary_ufunc(np.hypot)
# Comparison functions
@overload_elementwise_binary_ufunc(np.greater, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.greater_equal, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.less, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.less_equal, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.not_equal, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.equal, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.logical_and, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.logical_or, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.logical_xor, dtype=types.int8)
@overload_elementwise_binary_ufunc(np.maximum)
@overload_elementwise_binary_ufunc(np.minimum)
@overload_elementwise_binary_ufunc(np.fmax)
@overload_elementwise_binary_ufunc(np.fmin)
# Floating functions
# @overload_elementwise_binary_ufunc(np.ldexp) # not supported by numba?
def dummy_binary_ufunc(a, b):
    pass


def overload_elementwise_unary_ufunc(ufunc, name=None, dtype=None):
    """
    Wrapper for unary ufuncs that returns an array
    """
    if name is None:
        name = ufunc.__name__
    globals()[name] = ufunc

    def unary_elementwise_ufunc_impl(a):
        if isinstance(a, ArrayPointer):
            if dtype is None:
                nb_dtype = a.eltype
            else:
                nb_dtype = dtype

            def impl(a):
                sz = len(a)
                x = Array(sz, nb_dtype)
                for i in range(sz):
                    x[i] = nb_dtype(ufunc(a[i]))
                return x
            return impl

    decorate = extending.overload(ufunc)

    def wrapper(overload_func):
        return decorate(unary_elementwise_ufunc_impl)

    return wrapper


# Math operations
@overload_elementwise_unary_ufunc(np.negative)
@overload_elementwise_unary_ufunc(np.positive)
@overload_elementwise_unary_ufunc(np.absolute)
@overload_elementwise_unary_ufunc(np.fabs)
@overload_elementwise_unary_ufunc(np.rint)
# @overload_elementwise_unary_ufunc(np.absolute) # test?
# @overload_elementwise_unary_ufunc(np.conj) # test?
# @overload_elementwise_unary_ufunc(np.conjugate) # test?
@overload_elementwise_unary_ufunc(np.exp)
@overload_elementwise_unary_ufunc(np.exp2)
@overload_elementwise_unary_ufunc(np.log)
@overload_elementwise_unary_ufunc(np.log2)
@overload_elementwise_unary_ufunc(np.log10)
@overload_elementwise_unary_ufunc(np.expm1)
@overload_elementwise_unary_ufunc(np.log1p)
@overload_elementwise_unary_ufunc(np.sqrt)
@overload_elementwise_unary_ufunc(np.square)
# @overload_elementwise_unary_ufunc(np.cbrt) # not supported
@overload_elementwise_unary_ufunc(np.reciprocal)
# Bit-twiddling functions
@overload_elementwise_unary_ufunc(np.invert)
# trigonometric functions
@overload_elementwise_unary_ufunc(np.sin)
@overload_elementwise_unary_ufunc(np.cos)
@overload_elementwise_unary_ufunc(np.tan)
@overload_elementwise_unary_ufunc(np.arcsin)
@overload_elementwise_unary_ufunc(np.arccos)
@overload_elementwise_unary_ufunc(np.arctan)
@overload_elementwise_unary_ufunc(np.sinh)
@overload_elementwise_unary_ufunc(np.cosh)
@overload_elementwise_unary_ufunc(np.tanh)
@overload_elementwise_unary_ufunc(np.arcsinh)
@overload_elementwise_unary_ufunc(np.arccosh)
@overload_elementwise_unary_ufunc(np.arctanh)
@overload_elementwise_unary_ufunc(np.deg2rad)
@overload_elementwise_unary_ufunc(np.rad2deg)
# Comparison functions
@overload_elementwise_unary_ufunc(np.logical_not, dtype=types.int8)
# Floating functions
@overload_elementwise_unary_ufunc(np.isfinite, dtype=types.int8)
@overload_elementwise_unary_ufunc(np.isinf, dtype=types.int8)
@overload_elementwise_unary_ufunc(np.isnan, dtype=types.int8)
@overload_elementwise_unary_ufunc(np.fabs, dtype=types.double)
@overload_elementwise_unary_ufunc(np.floor, dtype=types.double)
@overload_elementwise_unary_ufunc(np.ceil, dtype=types.double)
@overload_elementwise_unary_ufunc(np.trunc, dtype=types.double)
# not supported?
# @overload_elementwise_unary_ufunc(np.isnat, dtype=types.int8)
# @overload_elementwise_unary_ufunc(np.signbit, dtype=types.int8)
# @overload_elementwise_unary_ufunc(np.spacing, dtype=types.double)
def dummy_unary_ufunc(a):
    pass


def heaviside(x1, x2):
    pass


@extending.overload(heaviside)
def impl_np_heaviside(x1, x2):
    if isinstance(x1, ArrayPointer):
        nb_dtype = types.double

        def impl(x1, x2):
            sz = len(x1)
            r = Array(sz, nb_dtype)
            for i in range(sz):
                if x1[i] < 0:
                    r[i] = nb_dtype(0)
                elif x1[i] == 0:
                    r[i] = nb_dtype(x2)
                else:
                    r[i] = nb_dtype(1)
            return r
        return impl
