"""Implement Omnisci Column type support

Omnisci Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['ColumnPointer', 'OutputColumn', 'Column',
           'OmnisciOutputColumnType', 'OmnisciColumnType',
           'output_column_type_converter', 'column_type_converter',
           'table_function_sizer_type_converter']

from rbc import typesystem
from rbc.utils import get_version
from .omnisci_buffer import (
    BufferPointer, Buffer, BufferPointerModel,
    buffer_type_converter, OmnisciBufferType,
)

if get_version('numba') >= (0, 49):
    from numba.core import datamodel
else:
    from numba import datamodel


class OmnisciColumnType(OmnisciBufferType):
    """Omnisci Column type for RBC typesystem.
    """


class OmnisciOutputColumnType(OmnisciBufferType):
    """Omnisci OutputColumn type for RBC typesystem.

    OutputColumn is the same as Column but introduced to distinguish
    the input and output arguments of UDTFs.
    """


class ColumnPointer(BufferPointer):
    """Type class for pointers to :code:`Omnisci Column<T>` structure."""


class Column(Buffer):
    pass


class OutputColumn(Column):
    pass


@datamodel.register_default(ColumnPointer)
class ColumnPointerModel(BufferPointerModel):
    pass


def column_type_converter(target_info, obj):
    """Return Type instance corresponding to Omnisci :code:`Column<T>` type.

    :code:`Omnisci Column<T>` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Column {
        T* ptr;
        size_t sz;
      }

    See :code:`buffer_type_converter` for details.
    """
    return buffer_type_converter(
        target_info, obj, OmnisciColumnType, 'Column', ColumnPointer,
        extra_members=[])


def output_column_type_converter(target_info, obj):
    """Return Type instance corresponding to Omnisci :code:`OutputColumn<T>` type.

    See :code:`column_type_converter` for implementation detail.
    """
    return buffer_type_converter(
        target_info, obj, OmnisciOutputColumnType, 'OutputColumn',
        ColumnPointer, extra_members=[])


def table_function_sizer_type_converter(target_info, obj):
    """Return Type instance corresponding to sizer argument of a
    user-defined table function.
    """
    if not isinstance(obj, typesystem.Type):
        raise NotImplementedError(type(obj))
    if obj.is_atomic:
        sizer_name = obj[0]
        sizer_types = ['RowMultiplier', 'ConstantParameter', 'Constant']
        if sizer_name in sizer_types:
            return typesystem.Type.fromstring(f'int32|sizer={sizer_name}',
                                              target_info=target_info)
