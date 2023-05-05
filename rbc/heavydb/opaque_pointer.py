"""
Implements a common interface for pointers to structs defined in HeavyDB.
    + TableFunctionManager
    + RowFunctionManager
    + String Dictionary Proxy
"""

__all__ = ['OpaquePtrNumbaType', 'HeavyDBOpaquePtr']


from numba.core import types as nb_types
from numba.core import extending, datamodel

from rbc import typesystem


class OpaquePtrNumbaType(nb_types.CPointer):
    """
    """
    def __init__(self) -> None:
        super().__init__(dtype=nb_types.int8)


class HeavyDBOpaquePtr(typesystem.Type):

    def register_datamodel(self):
        name = self.type_name
        data_model = type(name+'Model',
                          (datamodel.models.PointerModel,),
                          dict())
        extending.register_model(self.numba_type)(data_model)

    def postprocess_type(self):
        self.register_datamodel()
        return super().postprocess_type()

    @property
    def numba_type(self):
        raise NotImplementedError()

    @property
    def type_name(self):
        raise NotImplementedError()

    def tonumba(self, bool_is_int8=None):
        return self.numba_type()

    @property
    def supported_devices(self):
        return {'CPU', 'GPU'}
