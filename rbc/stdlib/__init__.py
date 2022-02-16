import functools
from numba.core import extending
from rbc.omnisci_backend import Array, ArrayPointer
from rbc import typesystem


ADDRESS = ("https://data-apis.org/array-api/latest/API_specification"
           "/generated/signatures.{0}.{1}.html"
           "#signatures.{0}.{1}")


class Expose:
    def __init__(self, globals, module_name):
        self._globals = globals
        self.module_name = module_name

    def create_function(self, func_name):
        s = f'def {func_name}(*args, **kwargs): pass'
        exec(s, self._globals)
        fn = self._globals.get(func_name)
        return fn

    def implements(self, func_name):
        fn = self.create_function(func_name)
        decorate = extending.overload(fn)

        def wrapper(overload_func):
            overload_func.__doc__ = (
                f"`array-api '{func_name}' "
                f"doc <{ADDRESS.format(self.module_name, func_name)}>`_")
            functools.update_wrapper(fn, overload_func)
            return decorate(overload_func)

        return wrapper

    def not_implemented(self, func_name):
        s = f'def {func_name}(*args, **kwargs): pass'
        exec(s, self._globals)

        fn = self._globals.get(func_name)

        def wraps(func):
            func.__doc__ = "❌ Not implemented"
            functools.update_wrapper(fn, func)
            return func
        return wraps


class UfuncExpose(Expose):

    @classmethod
    def determine_dtype(a, dtype):
        if isinstance(a, ArrayPointer):
            return a.eltype if dtype is None else dtype
        else:
            return a if dtype is None else dtype

    @classmethod
    def determine_input_type(argty):
        if isinstance(argty, ArrayPointer):
            return Expose.determine_input_type(argty.eltype)

        if argty == typesystem.boolean8:
            return bool
        else:
            return argty

    def overload_elementwise_binary_ufunc(self, ufunc, ufunc_name=None, dtype=None):
        """
        Wrapper for binary ufuncs that returns an array
        """
        if ufunc_name is None:
            ufunc_name = ufunc.__name__
        # self._globals[ufunc_name] = ufunc

        def binary_ufunc_impl(a, b):
            typA = Expose.determine_input_type(a)
            typB = Expose.determine_input_type(b)

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
                nb_dtype = Expose.determine_dtype(a, dtype)

                def impl(a, b):
                    return binary_impl(a, b, nb_dtype)
                return impl
            elif isinstance(a, ArrayPointer):
                nb_dtype = Expose.determine_dtype(a, dtype)
                other_dtype = b

                def impl(a, b):
                    b = broadcast(b, len(a), other_dtype)
                    return binary_impl(a, b, nb_dtype)
                return impl
            elif isinstance(b, ArrayPointer):
                nb_dtype = Expose.determine_dtype(b, dtype)
                other_dtype = a

                def impl(a, b):
                    a = broadcast(a, len(b), other_dtype)
                    return binary_impl(a, b, nb_dtype)
                return impl
            else:
                nb_dtype = Expose.determine_dtype(a, dtype)

                def impl(a, b):
                    cast_a = typA(a)
                    cast_b = typB(b)
                    return nb_dtype(ufunc(cast_a, cast_b))
                return impl

        decorate = extending.overload(ufunc)
        fn = self.create_function(ufunc_name)
        fn.__doc__ = (
            f"`array-api '{ufunc_name}' "
            f"doc <{ADDRESS.format(self.module_name, ufunc_name)}>`_")

        def wrapper(overload_func):
            # https://chriswarrick.com/blog/2018/09/20/python-hackery-merging-signatures-of-two-python-functions/
            fn.__wrapped__ = overload_func
            return decorate(binary_ufunc_impl)

        return wrapper
