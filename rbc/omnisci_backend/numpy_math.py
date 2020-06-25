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


def overload_binary_ufunc(ufunc):
    name = ufunc.__name__
    # s = f'def {name}(*args, **kwargs): pass'
    # exec(s, globals())
    globals()[name] = ufunc

    def binary_ufunc_impl(a, b):
        # XXX: raise error if len(a) != len(b)
        breakpoint()
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
        return decorate(overload_binary_ufunc)

    return wrapper

@overload_binary_ufunc(np.add)
def dummy_binary_ufunc(a, b):
    pass

        