'''RBC TextEncodingDict type that corresponds to HeavyDB type TEXT ENCODED DICT.
'''

__all__ = ['HeavyDBTextEncodingDictType', 'TextEncodingDict']

from .metatype import HeavyDBMetaType
from rbc import typesystem
from numba.core import types as nb_types


class TextEncodingDictNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='TextEncodingDictNumbaType')


class HeavyDBTextEncodingDictType(typesystem.Type):
    """HeavyDB Text Encoding Dict type for RBC typesystem.
    """

    @property
    def __typesystem_type__(self):
        return typesystem.Type('int32')

    def postprocess_type(self):
        return self.params(shorttypename='TextEncodingDict')


class TextEncodingDict(object, metaclass=HeavyDBMetaType):
    """
    RBC ``TextEncodingDict`` type that corresponds to HeavyDB type TEXT ENCODED DICT.


    HeavyDB TextEncodingDict behaves like an int32_t.

    .. code-block:: c

        {
            int32_t value;
        }
    """
    pass
