import functools
from numba.core import extending


ADDRESS = ("https://data-apis.org/array-api/latest/API_specification"
           "/generated/signatures.{0}.{1}.html"
           "#signatures.{0}.{1}")


class Expose:
    def __init__(self, globals, module_name):
        self._globals = globals
        self.module_name = module_name

    def implements(self, func_name):
        s = f'def {func_name}(*args, **kwargs): pass'
        exec(s, self._globals)

        fn = self._globals.get(func_name)
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
