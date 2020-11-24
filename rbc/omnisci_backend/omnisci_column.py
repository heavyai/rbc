"""Implement Omnisci Column type support

Omnisci Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['ColumnPointer', 'OutputColumn', 'Column',
           'OmnisciOutputColumnType', 'OmnisciColumnType',
           'OmnisciCursorType', 'cursor_type_converter',
           'output_column_type_converter', 'column_type_converter',
           'table_function_sizer_type_converter']


from llvmlite import ir
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

int32_t = ir.IntType(32)


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


def column_type_converter(obj):
    """Return Type instance corresponding to Omnisci :code:`Column<T>` type.

    :code:`Omnisci Column<T>` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Column {
        T* ptr;
        size_t sz;
        int64_t table_id;
      }

    See :code:`buffer_type_converter` for details.
    """
    return buffer_type_converter(
        obj, OmnisciColumnType, 'Column', ColumnPointer,
        extra_members=[
            typesystem.Type.fromstring('int64_t table_id')])


def output_column_type_converter(obj):
    """Return Type instance corresponding to Omnisci :code:`OutputColumn<T>` type.

    See :code:`column_type_converter` for implementation detail.
    """
    return buffer_type_converter(
        obj, OmnisciOutputColumnType, 'OutputColumn',
        ColumnPointer,
        extra_members=[
            typesystem.Type.fromstring('int64_t table_id')])


def table_function_sizer_type_converter(obj):
    """Return Type instance corresponding to sizer argument of a
    user-defined table function.
    """
    if not isinstance(obj, typesystem.Type):
        raise NotImplementedError(type(obj))
    if obj.is_atomic:
        sizer_name = obj[0]
        sizer_types = ['RowMultiplier', 'ConstantParameter', 'Constant']
        if sizer_name in sizer_types:
            return typesystem.Type.fromstring(f'int32|sizer={sizer_name}')


class OmnisciCursorType(typesystem.Type):

    @property
    def as_consumed_args(self):
        return list(self)


def cursor_type_converter(obj):
    """Return Type instance corresponding to cursor argument of a
    user-defined table function.
    """
    if not isinstance(obj, typesystem.Type):
        raise NotImplementedError(type(obj))
    name, params = obj.get_name_and_parameters()
    if name == 'Cursor':
        new_params = []
        for p in params:
            p = typesystem.Type.fromstring(p)
            if p.is_float or p.is_int or p.is_bool:
                p = typesystem.Type.fromstring(f'Column<{p}>')
            if not isinstance(p, OmnisciColumnType):
                raise TypeError(
                    f'expected OmnisciColumnType|float|int|bool as'
                    f' OmnisciCursorType argument but got `{p}`')
            new_params.append(p)
        cursor_type = OmnisciCursorType(*new_params)
        cursor_type._params['clsname'] = name
        return cursor_type
