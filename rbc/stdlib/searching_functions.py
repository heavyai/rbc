"""
Array API specification for searching functions.

https://data-apis.org/array-api/latest/API_specification/searching_functions.html
"""
from rbc.heavydb import Array, ArrayPointer, type_can_asarray
from rbc.stdlib import Expose

__all__ = ["argmax", "argmin", "nonzero", "where"]

expose = Expose(globals(), "searching_functions")


@expose.implements("argmax")
def _array_api_argmax(x):
    """
    Returns the indices of the maximum values along a specified axis.
    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        def impl(x):
            argmax = array_api.max(x)
            lst = []
            for i in range(len(x)):
                if not x.is_null(i) and array_api.equal(x[i], argmax):
                    lst.append(i)
                    break
            return Array(lst)
        return impl


@expose.implements("argmin")
def _array_api_argmin(x):
    """
    Returns the indices of the minimum values along a specified axis.
    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        def impl(x):
            argmin = array_api.min(x)
            lst = []
            for i in range(len(x)):
                if x.is_null(i):
                    continue

                if array_api.equal(x[i], argmin):
                    lst.append(i)
                    break
            return Array(lst)
        return impl


@expose.implements("nonzero")
def _array_api_nonzero(x):
    """
    Returns the indices of the array elements which are non-zero.
    """
    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer):
        def impl(x):
            lst = []
            for i in range(len(x)):
                if not x.is_null(i) and array_api.not_equal(x[i], x.dtype(0)):
                    lst.append(i)
            return Array(lst)
        return impl


@expose.implements("where")
def _array_api_where(condition, x1, x2):
    """
    Returns elements chosen from `x1` or `x2` depending on `condition`.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()
    >>> import numpy as np

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('int32[](int32[])')
    ... def rbc_where(a):
    ...     return array_api.where(a < 2, a, -1)
    >>> a = np.arange(5, dtype=np.int32)
    >>> rbc_where(a).execute()
    array([ 0,  1, -1, -1, -1], dtype=int32)

    >>> @heavydb('int32[](int32[])')
    ... def rbc_where(a):
    ...     return array_api.where([True, False], 2, a)
    >>> a = np.asarray([111, 222], dtype=np.int32)
    >>> rbc_where(a).execute()
    array([  2, 222], dtype=int32)
    """
    from rbc.stdlib import array_api

    # XXX: raise error if len(condition) != len(x1) != len(x2)
    if type_can_asarray(condition):
        if all([isinstance(a, ArrayPointer) for a in (x1, x2)]):
            def impl(condition, x1, x2):
                dtype = array_api.result_type(x1, x2)
                sz = len(x1)
                r = array_api.empty(sz, dtype=dtype)
                for i in range(sz):
                    if condition[i]:
                        if x1.is_null(i):
                            r.set_null(i)
                        else:
                            r[i] = r.dtype(x1[i])
                    else:
                        if x2.is_null(i):
                            r.set_null(i)
                        else:
                            r[i] = r.dtype(x2[i])
                return r
            return impl
        else:
            if isinstance(x1, ArrayPointer):
                def impl(condition, x1, x2):
                    x2 = array_api.full_like(x1, fill_value=x2)
                    return array_api.where(condition, x1, x2)
            else:
                def impl(condition, x1, x2):
                    x1 = array_api.full_like(x2, fill_value=x1)
                    return array_api.where(condition, x1, x2)
            return impl
