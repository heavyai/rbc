
__all__ = ['HeavyDBAbstractType']

from abc import abstractmethod
from rbc import typesystem


class HeavyDBAbstractType(typesystem.Type):
    """The type where all HeavyDB types must inherit from
    """

    @property
    @abstractmethod
    def pass_by_value(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def numba_type(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def numba_pointer_type(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def element_type(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def buffer_extra_members(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def custom_params(self):
        raise NotImplementedError

    @abstractmethod
    def tonumba(self, bool_is_int8=None):
        raise NotImplementedError
