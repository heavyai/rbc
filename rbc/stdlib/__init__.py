import functools
from enum import Enum
from numba.core import extending
from rbc.heavydb import Array, ArrayPointer
from rbc import typesystem, errors


ARRAY_API_ADDRESS = ("https://data-apis.org/array-api/latest/API_specification"
                     "/generated/signatures.{0}.{1}.html"
                     "#signatures.{0}.{1}")
NUMPY_API_ADDRESS = ("https://numpy.org/doc/stable/reference/generated/numpy.{0}.html")
ADDRESS = ARRAY_API_ADDRESS


class API(Enum):
    NUMPY_API = 0
    ARRAY_API = 1


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


class Expose:
    def __init__(self, globals, module_name):
        self._globals = globals
        self.module_name = module_name

    def create_function(self, func_name):
        s = f'def {func_name}(*args, **kwargs): pass'
        exec(s, self._globals)
        fn = self._globals.get(func_name)
        return fn

    def format_docstring(self, ov_func, func_name, api):
        original_docstring = ov_func.__doc__
        if api == API.NUMPY_API:
            # Numpy
            link = (
                f"`NumPy '{func_name}' "
                f"doc <{NUMPY_API_ADDRESS.format(func_name)}>`_")
        else:
            # Array API
            link = (
                f"`Array-API '{func_name}' "
                f"doc <{ARRAY_API_ADDRESS.format(self.module_name, func_name)}>`_")

        if original_docstring is not None:
            new_doctring = f"{original_docstring}\n\n{link}"
        else:
            new_doctring = link
        return new_doctring

    def implements(self, func_name, api=API.ARRAY_API):
        fn = self.create_function(func_name)
        decorate = extending.overload(fn)

        def wrapper(overload_func):
            overload_func.__doc__ = self.format_docstring(overload_func, func_name, api)
            functools.update_wrapper(fn, overload_func)
            return decorate(overload_func)

        return wrapper

    def not_implemented(self, func_name, api=API.ARRAY_API):
        fn = self.create_function(func_name)
        decorate = extending.overload(fn)

        def unimplemented(*args, **kwargs):
            raise errors.NumbaNotImplementedError(f'Function "{func_name}" is not supported.\n'
                                                  'Please, open a ticket on the RBC project '
                                                  'and report this error if you need support for '
                                                  'this function.')

        def wraps(overload_func):
            original_doc = self.format_docstring(overload_func, func_name, api)
            overload_func.__doc__ = f"❌ Not implemented\n{original_doc}"
            functools.update_wrapper(fn, overload_func)
            return decorate(unimplemented)
        return wraps


class BinaryUfuncExpose(Expose):

    def implements(self, ufunc, ufunc_name=None, dtype=None, api=API.ARRAY_API):
        """
        Wrapper for binary ufuncs that returns an array
        """
        if ufunc_name is None:
            ufunc_name = ufunc.__name__

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

        fn = self.create_function(ufunc_name)

        def wrapper(overload_func):
            overload_func.__doc__ = self.format_docstring(overload_func, ufunc_name, api)
            functools.update_wrapper(fn, overload_func)

            decorate = extending.overload(fn)
            return decorate(binary_ufunc_impl)

        return wrapper


class UnaryUfuncExpose(BinaryUfuncExpose):

    def implements(self, ufunc, ufunc_name=None, dtype=None, api=API.ARRAY_API):
        """
        Wrapper for unary ufuncs that returns an array
        """
        if ufunc_name is None:
            ufunc_name = ufunc.__name__

        def unary_ufunc_impl(a):
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

        fn = self.create_function(ufunc_name)

        def wrapper(overload_func):
            overload_func.__doc__ = self.format_docstring(overload_func, ufunc_name, api)
            functools.update_wrapper(fn, overload_func)

            decorate = extending.overload(fn)
            return decorate(unary_ufunc_impl)

        return wrapper
