"""
Implements a common interface for pointers to structs defined in HeavyDB.
    + TableFunctionManager
    + RowFunctionManager
    + String Dictionary Proxy
"""

__all__ = ['ProxyNumbaType', 'HeavyDBProxy']


from numba.core import types as nb_types

from rbc import typesystem


class ProxyNumbaType(nb_types.CPointer):
    """
    """
    def __init__(self) -> None:
        super().__init__(dtype=nb_types.int8)


class HeavyDBProxy(typesystem.Type):

    @property
    def numba_type(self):
        raise NotImplementedError()

    @property
    def typename(self):
        raise NotImplementedError()

    def tonumba(self, bool_is_int8=None):
        return self.numba_type()
