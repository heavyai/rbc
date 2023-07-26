"""
Array API specification for utility functions.

https://data-apis.org/array-api/latest/API_specification/utility_functions.html
"""
from rbc.stdlib import Expose
from rbc.heavydb import ArrayPointer

__all__ = ["all", "any"]

expose = Expose(globals(), "utility_functions")


@expose.implements("all")
def _array_api_all(x):
    """
    Tests whether all input array elements evaluate to `True` along a specified axis.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('bool(int64[])')
    ... def array_api_all(arr):
    ...     return array_api.all(arr)
    >>> array_api_all([0, 1]).execute()
    False
    >>> array_api_all([1]).execute()
    True
    """
    if isinstance(x, ArrayPointer):
        def impl(x):
            for i in range(len(x)):
                if x.is_null(i):
                    continue

                if not bool(x[i]):
                    return False
            return True
        return impl


@expose.implements("any")
def _array_api_any(x):
    """
    Tests whether any input array element evaluates to `True` along a specified axis.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('bool(int64[])')
    ... def array_api_any(arr):
    ...     return array_api.any(arr)
    >>> array_api_any([0, 1]).execute()
    True
    >>> array_api_any([0]).execute()
    False

    """
    if isinstance(x, ArrayPointer):
        def impl(x):
            if x.is_null():
                return None

            if len(x) == 0:
                return False

            for i in range(len(x)):
                if x.is_null(i):
                    continue

                if bool(x[i]) is True:
                    return True
            return False
        return impl
