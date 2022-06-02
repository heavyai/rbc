"""Implement Buffer type as a base class to HeavyDB Array and Column types.

HeavyDB Buffer represents the following structure:

  template<typename T>
  struct Buffer {
    T* ptr;
    size_t sz;
    ...
  }

that is, a structure that has at least two members where the first is
a pointer to some data type and the second is the size of the buffer.

This module implements the support for the following Python operators:

  len
  __getitem__
  __setitem__

to provide a minimal functionality for accessing and manipulating the
HeavyDB buffer objects from UDF/UDTFs.
"""


from rbc import typesystem
from rbc.heavydb import HeavyDBMetaType


__all__ = ['HeavyDBTimestampType', 'Timestamp']


class Timestamp(metaclass=HeavyDBMetaType):
    pass


class HeavyDBTimestampType(typesystem.Type):
    """Typesystem type class for HeavyDB buffer structures.
    """
    @property
    def __typesystem_type__(self):
        return typesystem.Type('int64')

    def tostring(self, use_typename=False, use_annotation=True, use_name=True,
                 use_annotation_name=False, _skip_annotation=False):
        return 'Timestamp'
