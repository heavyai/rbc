"""Implement Omnisci Column type support

Omnisci Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['OutputColumn', 'Column', 'OmnisciOutputColumnType', 'OmnisciColumnType',
           'OmnisciCursorType']

from rbc import typesystem
from .omnisci_buffer import Buffer, OmnisciBufferType


class OmnisciColumnType(OmnisciBufferType):
    """Omnisci Column type for RBC typesystem.
    """
    pass_by_value = True


class OmnisciOutputColumnType(OmnisciColumnType):
    """Omnisci OutputColumn type for RBC typesystem.

    OutputColumn is the same as Column but introduced to distinguish
    the input and output arguments of UDTFs.
    """


class Column(Buffer):
    pass


class OutputColumn(Column):
    pass


class OmnisciCursorType(typesystem.Type):

    @classmethod
    def preprocess_args(cls, args):
        assert len(args) == 1
        params = []
        for p in args[0]:
            if not isinstance(p, OmnisciColumnType):
                p = OmnisciColumnType((p,))
            params.append(p)
        return (tuple(params),)

    @property
    def as_consumed_args(self):
        return self[0]


typesystem.Type.alias(
    Cursor='OmnisciCursorType',
    Column='OmnisciColumnType',
    OutputColumn='OmnisciOutputColumnType',
    RowMultiplier='int32|sizer=RowMultiplier',
    ConstantParameter='int32|sizer=ConstantParameter',
    Constant='int32|sizer=Constant')
