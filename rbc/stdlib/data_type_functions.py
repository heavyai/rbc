"""
Array API specification for data type functions.

https://data-apis.org/array-api/latest/API_specification/data_type_functions.html
"""
import numpy as np
from typing import NamedTuple
from numba.core import types, extending
from numba.np.numpy_support import as_dtype, from_dtype
from rbc.errors import NumbaTypeError
from rbc.heavydb import ArrayPointer
from rbc.stdlib import Expose
from rbc.typesystem import Boolean8

__all__ = [
    "astype",
    "broadcast_arrays",
    "broadcast_to",
    "can_cast",
    "finfo",
    "iinfo",
    "result_type",
]

expose = Expose(globals(), "data_type_functions")


@expose.implements("astype")
def _array_api_astype(x, dtype, copy=True):
    """
    Copies an array to a specified data type irrespective of
    `Type Promotion Rules <https://data-apis.org/array-api/latest/API_specification/type_promotion.html#type-promotion>`_ rules.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('bool[](int64[])')
    ... def astype(x):
    ...     return array_api.astype(x, array_api.bool)
    >>> astype([1, 0, 3]).execute()
    array([ True, False,  True])

    """

    from rbc.stdlib import array_api

    if isinstance(x, ArrayPointer) and isinstance(dtype, types.DTypeSpec):
        def impl(x, dtype, copy=True):
            r = array_api.empty_like(x, dtype=dtype)
            for i in range(len(r)):
                if x.is_null(i):
                    r.set_null(i)
                else:
                    r[i] = dtype(x[i])
            return r
        return impl


@expose.not_implemented("broadcast_arrays")
def _array_api_broadcast_arrays(*arrays):
    """
    Broadcasts one or more arrays against one another.
    """
    pass


@expose.not_implemented("broadcast_to")
def _array_api_broadcast_to(x, shape):
    """
    Broadcasts an array to a specified shape.
    """
    pass


@expose.implements("can_cast")
def _array_api_can_cast(from_, to):
    """
    Determines if one data type can be cast to another data type according
    `Type Promotion Rules <https://data-apis.org/array-api/latest/API_specification/type_promotion.html#type-promotion>`_ rules.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('bool(int64)')
    ... def can_cast(_):
    ...     return array_api.can_cast(array_api.int8, array_api.int32)
    >>> can_cast(0).execute()
    True

    >>> @heavydb('bool(int64)')
    ... def can_cast(_):
    ...     return array_api.can_cast(array_api.float32, array_api.bool)
    >>> can_cast(0).execute()
    False

    """
    if all([isinstance(a, types.DTypeSpec) for a in (from_, to)]):
        f, t = as_dtype(from_.dtype), as_dtype(to.dtype)
        can_cast = np.can_cast(f, t)

        def impl(from_, to):
            return can_cast
        return impl


@expose.implements("finfo")
def _array_api_finfo(type):
    """
    Machine limits for floating-point data types.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('float64(int64)')
    ... def array_api_finfo(_):
    ...     return array_api.finfo(array_api.float64).max
    >>> array_api_finfo(0).execute()
    inf

    """
    class FInfo(NamedTuple):
        bits: int
        eps: float
        min: float
        max: float
        smallest_normal: float
        dtype: types.DTypeSpec

    if isinstance(type, types.DTypeSpec) and type.dtype in (types.float32, types.float64):
        finfo = np.finfo(as_dtype(type.dtype))
        finfo_nt = FInfo(finfo.bits,
                         finfo.resolution,
                         finfo.min,
                         finfo.max,
                         finfo.smallest_normal,
                         type)

        def impl(type):
            return finfo_nt
        return impl


@expose.implements("iinfo")
def _array_api_iinfo(type):
    """
    Machine limits for integer data types.

    Examples
    --------
    IGNORE:
    >>> from rbc.heavydb import global_heavydb_singleton
    >>> heavydb = next(global_heavydb_singleton)
    >>> heavydb.unregister()

    IGNORE

    >>> from rbc.stdlib import array_api
    >>> @heavydb('int64(int64)')
    ... def array_api_iinfo(_):
    ...     return array_api.iinfo(array_api.int8).min
    >>> array_api_iinfo(0).execute()
    -127

    >>> @heavydb('int64(int64)')
    ... def array_api_iinfo(_):
    ...     return array_api.iinfo(array_api.int16).bits
    >>> array_api_iinfo(0).execute()
    16

    """
    class IInfo(NamedTuple):
        bits: int
        min: int
        max: int
        dtype: types.DTypeSpec

    if isinstance(type, types.DTypeSpec):
        iinfo = np.iinfo(as_dtype(type.dtype))
        # iinfo.min is used to represent null values. Thus, the minimum value
        # one can use is "iinfo.min + 1"
        iinfo_nt = IInfo(iinfo.bits, iinfo.min+1, iinfo.max, type)

        def impl(type):
            return iinfo_nt
        return impl


@expose.not_implemented("isdtype")
def _array_api_isdtype(dtype, kind):
    """
    Returns a boolean indicating whether a provided dtype is of a specified
    data type "kind".
    """
    pass


@expose.implements("result_type")
def _array_api_result_type(*arrays_and_dtypes):
    """
    Returns the dtype that results from applying the type promotion
    rules (see `Type Promotion Rules <https://data-apis.org/array-api/latest/API_specification/type_promotion.html#type-promotion>`_)
    to the arguments.
    """
    dts = []
    for arg in arrays_and_dtypes:
        if isinstance(arg, ArrayPointer):
            eltype = types.int8 if isinstance(arg.eltype, Boolean8) else arg.eltype
            dts.append(as_dtype(eltype))
        elif isinstance(arg, types.DTypeSpec):
            dts.append(as_dtype(arg.dtype))
        else:
            msg = ('Expect Array or dtype when calling array_api.result_type. '
                   f'Got {arg}')
            raise NumbaTypeError(msg)

    dt = from_dtype(np.result_type(*dts))

    def impl(*arrays_and_dtypes):
        return dt
    return impl


@extending.overload(str)
def ol_str(type):
    if isinstance(type, types.DTypeSpec):
        s = str(type.dtype)

        def impl(type):
            return s
        return impl
