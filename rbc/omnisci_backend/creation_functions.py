"""
https://data-apis.org/array-api/latest/API_specification/creation_functions.html
"""

from rbc import typesystem
from .omnisci_array import Array, ArrayPointer
from .helper import Expose
from numba import njit
from numba.core import extending, types

__all__ = [
    'full', 'full_like', 'empty_like', 'empty', 'zeros', 'zeros_like',
    'ones', 'ones_like', 'array', 'cumsum'
]


expose = Expose(globals(), 'creation_functions')


@expose.not_implemented('arange')
def _omnisci_arange(start, stop=None, step=1, dtype=None, device=None):
    pass


@expose.not_implemented('asarray')
def asarray(obj, dtype=None, device=None, copy=None):
    pass


@expose.not_implemented('eye')
def eye(n_rows, n_cols=None, k=0, dtype=None, device=None):
    pass


@expose.not_implemented('from_dlpack')
def from_dlpack(x, /):
    pass


@expose.not_implemented('linspace')
def linspace(start, stop, num, dtype=None, device=None, endpoint=True):
    pass


@expose.not_implemented('meshgrid')
def meshgrid(*arrays, indexing='xy'):
    pass


@expose.not_implemented('tril')
def tril(x, k=0):
    pass


@expose.not_implemented('triu')
def triu(x, k=0):
    pass


@expose.implements('full')
def _omnisci_np_full(shape, fill_value, dtype=None):

    # XXX: dtype should be infered from fill_value
    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    def impl(shape, fill_value, dtype=None):
        a = Array(shape, nb_dtype)
        a.fill(nb_dtype(fill_value))
        return a
    return impl


@expose.implements('full_like')
def _omnisci_np_full_like(a, fill_value, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        def impl(a, fill_value, dtype=None):
            sz = len(a)
            other = Array(sz, nb_dtype)
            other.fill(nb_dtype(fill_value))
            return other
        return impl


@expose.implements('empty_like')
def _omnisci_np_empty_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        def impl(a, dtype=None):
            other = Array(0, nb_dtype)
            other.set_null()
            return other
        return impl


@expose.implements('empty')
def _omnisci_np_empty(shape, dtype=None):
    """

    """

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    def impl(shape, dtype=None):
        arr = Array(shape, nb_dtype)
        arr.set_null()
        return arr
    return impl


@expose.implements('zeros')
def _omnisci_np_zeros(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@expose.implements('zeros_like')
def _omnisci_np_zeros_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

        fill_value = False if isinstance(nb_dtype, types.Boolean) else 0

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@expose.implements('ones')
def _omnisci_np_ones(shape, dtype=None):

    if dtype is None:
        nb_dtype = types.double
    else:
        nb_dtype = typesystem.Type.fromobject(dtype).tonumba()

    fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

    def impl(shape, dtype=None):
        return full(shape, fill_value, nb_dtype)  # noqa: F821
    return impl


@expose.implements('ones_like')
def _omnisci_np_ones_like(a, dtype=None):
    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = dtype

        fill_value = True if isinstance(nb_dtype, types.Boolean) else 1

        def impl(a, dtype=None):
            return full_like(a, fill_value, nb_dtype)  # noqa: F821
        return impl


@expose.implements('array')
def _omnisci_np_array(a, dtype=None):

    @njit
    def _omnisci_array_non_empty_copy(a, nb_dtype):
        """Implement this here rather than inside "impl".
        LLVM DCE pass removes everything if we implement stuff inside "impl"
        """
        other = Array(len(a), nb_dtype)
        for i in range(len(a)):
            other[i] = a[i]
        return other

    if isinstance(a, ArrayPointer):
        if dtype is None:
            nb_dtype = a.eltype
        else:
            nb_dtype = dtype

        def impl(a, dtype=None):
            if a.is_null():
                return empty_like(a)  # noqa: F821
            else:
                return _omnisci_array_non_empty_copy(a, nb_dtype)
        return impl


@extending.overload_method(ArrayPointer, 'fill')
def _omnisci_array_fill(x, v):
    if isinstance(x, ArrayPointer):
        def impl(x, v):
            for i in range(len(x)):
                x[i] = v
        return impl


@expose.implements('cumsum')
def _omnisci_np_cumsum(a):
    if isinstance(a, ArrayPointer):
        eltype = a.eltype

        def impl(a):
            sz = len(a)
            out = Array(sz, eltype)
            out[0] = a[0]
            for i in range(sz):
                out[i] = out[i-1] + a[i]
            return out
        return impl
