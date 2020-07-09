"""Implement Omnisci Column type support

Omnisci Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['ColumnPointer', 'Column', 'column_type_converter']

from rbc.utils import get_version
from .omnisci_buffer import (BufferPointer, Buffer, BufferPointerModel,
                             buffer_type_converter)
if get_version('numba') >= (0, 49):
    from numba.core import datamodel
else:
    from numba import datamodel


class ColumnPointer(BufferPointer):
    """Type class for pointers to :code:`Omnisci Column<T>` structure."""


class Column(Buffer):
    pass


@datamodel.register_default(ColumnPointer)
class ColumnPointerModel(BufferPointerModel):
    pass


def column_type_converter(target_info, obj):
    """Return Type instance corresponding to :code:`Omnisci Column<T>` type.

    :code:`Omnisci Column<T>` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Column {
        T* ptr;
        size_t sz;
      }

    See :code:`buffer_type_converter` for details.
    """
    return buffer_type_converter(
        target_info, obj, 'Column', ColumnPointer,
        extra_members=[])
