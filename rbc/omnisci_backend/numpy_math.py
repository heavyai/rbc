import numpy as np
from rbc import typesystem
from rbc.irtools import printf
from .omnisci_array import Array, ArrayPointer
from rbc.utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import extending, types, \
        errors
    from numba.np import numpy_support
else:
    from numba import extending, types, \
        errors, numpy_support


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


def overload_unary_ufunc(ufunc, name=None):
    """
    Helper for unary ufuncs that returns an array
    """
    if name is None:
        name = ufunc.__name__
    globals()[name] = ufunc

    def binary_ufunc_impl(a, b):
        # XXX: raise error if len(a) != len(b)
        if isinstance(a, ArrayPointer) and isinstance(b, ArrayPointer):
            nb_dtype = a.eltype
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