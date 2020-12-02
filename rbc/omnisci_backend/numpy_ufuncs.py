import numpy as np
from .omnisci_array import Array, ArrayPointer
from rbc.utils import get_version
from .. import typesystem
if get_version('numba') >= (0, 49):
    from numba.core import extending, types
else:
    from numba import extending, types


def determine_dtype(a, dtype):
    if isinstance(a, ArrayPointer):
        return a.eltype if dtype is None else dtype
    else:
        return a if dtype is None else dtype


def determine_input_type(argty):
    if isinstance(argty, ArrayPointer):
        return determine_input_type(argty.eltype)

    if argty == typesystem.boolean8:
        return bool
    else:
        return argty


def overload_elementwise_binary_ufunc(ufunc, name=None, dtype=None):
    """
    Wrapper for binary ufuncs that returns an array
    """
    if name is None:
        name = ufunc.__name__
    globals()[name] = ufunc

    def binary_ufunc_impl(a, b):
        typA = determine_input_type(a)
        typB = determine_input_type(b)

        # XXX: raise error if len(a) != len(b)
        @extending.register_jitable(_nrt=False)
        def binary_impl(a, b, nb_dtype):
            sz = len(a)
            x = Array(sz, nb_dtype)
            for i in range(sz):
                cast_a = typA(a[i])
                cast_b = typB(b[i])
                x[i] = nb_dtype(ufunc(cast_a, cast_b))
            return x

        @extending.register_jitable(_nrt=False)
        def broadcast(e, sz, dtype):
            b = Array(sz, dtype)
            b.fill(e)
            return b

        if isinstance(a, ArrayPointer) and isinstance(b, ArrayPointer):
            nb_dtype = determine_dtype(a, dtype)

            def impl(a, b):
                return binary_impl(a, b, nb_dtype)
            return impl
        elif isinstance(a, ArrayPointer):
            nb_dtype = determine_dtype(a, dtype)
            other_dtype = b

            def impl(a, b):
                b = broadcast(b, len(a), other_dtype)
                return binary_impl(a, b, nb_dtype)
            return impl
        elif isinstance(b, ArrayPointer):
            nb_dtype = determine_dtype(b, dtype)
            other_dtype = a

            def impl(a, b):
                a = broadcast(a, len(b), other_dtype)
                return binary_impl(a, b, nb_dtype)
            return impl
        else:
            nb_dtype = determine_dtype(a, dtype)

            def impl(a, b):
                cast_a = typA(a)
                cast_b = typB(b)
                return nb_dtype(ufunc(cast_a, cast_b))
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
# @overload_elementwise_binary_ufunc(np.float_power)  # not supported by numba
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
@overload_elementwise_binary_ufunc(np.bitwise_not, name='bitwise_not')
@overload_elementwise_binary_ufunc(np.left_shift)
@overload_elementwise_binary_ufunc(np.right_shift)
# trigonometric functions
@overload_elementwise_binary_ufunc(np.arctan2)
@overload_elementwise_binary_ufunc(np.hypot)
# Comparison functions
@overload_elementwise_binary_ufunc(np.greater, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.greater_equal, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.less, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.less_equal, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.not_equal, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.equal, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.logical_and, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.logical_or, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.logical_xor, dtype=typesystem.boolean8)
@overload_elementwise_binary_ufunc(np.maximum)
@overload_elementwise_binary_ufunc(np.minimum)
@overload_elementwise_binary_ufunc(np.fmax)
@overload_elementwise_binary_ufunc(np.fmin)
# Floating functions
@overload_elementwise_binary_ufunc(np.nextafter)
@overload_elementwise_binary_ufunc(np.ldexp)
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
        nb_dtype = determine_dtype(a, dtype)
        typ = determine_input_type(a)

        if isinstance(a, ArrayPointer):
            def impl(a):
                sz = len(a)
                x = Array(sz, nb_dtype)
                for i in range(sz):
                    # Convert the value to type "typ"
                    cast = typ(a[i])
                    x[i] = nb_dtype(ufunc(cast))
                return x
            return impl
        else:
            def impl(a):
                # Convert the value to type typ
                cast = typ(a)
                return nb_dtype(ufunc(cast))
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
@overload_elementwise_unary_ufunc(np.sign)
@overload_elementwise_unary_ufunc(np.absolute)
@overload_elementwise_unary_ufunc(np.conj)
@overload_elementwise_unary_ufunc(np.conjugate)
@overload_elementwise_unary_ufunc(np.exp)
@overload_elementwise_unary_ufunc(np.exp2)
@overload_elementwise_unary_ufunc(np.log)
@overload_elementwise_unary_ufunc(np.log2)
@overload_elementwise_unary_ufunc(np.log10)
@overload_elementwise_unary_ufunc(np.expm1)
@overload_elementwise_unary_ufunc(np.log1p)
@overload_elementwise_unary_ufunc(np.sqrt)
@overload_elementwise_unary_ufunc(np.square)
# @overload_elementwise_unary_ufunc(np.cbrt)  # not supported by numba
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
@overload_elementwise_unary_ufunc(np.degrees)
@overload_elementwise_unary_ufunc(np.radians)
@overload_elementwise_unary_ufunc(np.deg2rad)
@overload_elementwise_unary_ufunc(np.rad2deg)
# Comparison functions
@overload_elementwise_unary_ufunc(np.logical_not, dtype=typesystem.boolean8)
# Floating functions
@overload_elementwise_unary_ufunc(np.isfinite, dtype=typesystem.boolean8)
@overload_elementwise_unary_ufunc(np.isinf, dtype=typesystem.boolean8)
@overload_elementwise_unary_ufunc(np.isnan, dtype=typesystem.boolean8)
@overload_elementwise_unary_ufunc(np.fabs, dtype=types.double)
@overload_elementwise_unary_ufunc(np.floor, dtype=types.double)
@overload_elementwise_unary_ufunc(np.ceil, dtype=types.double)
@overload_elementwise_unary_ufunc(np.trunc, dtype=types.double)
# not supported?
# @overload_elementwise_unary_ufunc(np.isnat, dtype=types.int8)
# issue 152:
@overload_elementwise_unary_ufunc(np.signbit, dtype=typesystem.boolean8)
@overload_elementwise_unary_ufunc(np.copysign)
@overload_elementwise_unary_ufunc(np.spacing, dtype=types.double)
def dummy_unary_ufunc(a):
    pass


def heaviside(x1, x2):
    pass


@extending.overload(heaviside)
def impl_np_heaviside(x1, x2):
    nb_dtype = types.double
    typA = determine_input_type(x1)
    typB = determine_input_type(x2)
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
