
"""Implement Heavydb Column<Array<T>> type support

Heavydb Column<Array<T>> type is the type of input/output column arguments in
UDTFs.
"""

# __all__ = ['OutputColumn', 'Column', 'HeavyDBOutputColumnType', 'HeavyDBColumnType',
#            'HeavyDBCursorType']

import operator
from rbc import typesystem
from .column import HeavyDBColumnType
from .buffer import BufferType, BufferPointer
from numba.core import extending
from numba.core import types as nb_types


class HeavyDBColumnArrayType(HeavyDBColumnType):

    def postprocess_type(self):
        return self

    @property
    def numba_type(self):
        return ColumnArrayType

    @property
    def numba_pointer_type(self):
        return ColumnArrayPointer

    @property
    def element_type(self):
        return typesystem.Type.fromstring('int8_t')


class ColumnArrayType(nb_types.Type):
    """Numba type class for HeavyDB buffer structures.
    """

    def __init__(self, dtype):
        breakpoint()

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.members[0].dtype

    # @property
    # def iterator_type(self):
    #     return BufferIteratorType(self)


class ColumnArrayPointer(nb_types.Type):
    """Numba type class for pointers to HeavyDB buffer structures.

    We are not deriving from CPointer because BufferPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        self.eltype = dtype.eltype  # buffer element dtype
        name = "%s[%s]*" % (type(self).__name__, dtype)
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


@extending.overload(operator.getitem)
def heavydb_buffer_getitem(x, i):
    if isinstance(x, ColumnArrayPointer):
        breakpoint()