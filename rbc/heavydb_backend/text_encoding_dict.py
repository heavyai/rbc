'''HeavyDB TextEncodingDict type that corresponds to HeavyDB type TEXT ENCODED DICT.
'''

__all__ = ['HeavyDBTextEncodingDictType', 'TextEncodingDict']

from .metatype import HeavyDBMetaType
from rbc import typesystem


class HeavyDBTextEncodingDictType(typesystem.Type):
    """HeavyDB Text Encoding Dict type for RBC typesystem.
    """

    @property
    def __typesystem_type__(self):
        return typesystem.Type('int32')


class TextEncodingDict(object, metaclass=HeavyDBMetaType):
    '''HeavyDB TextEncodingDict type that corresponds to HeavyDB type TEXT ENCODED DICT.
    HeavyDB TextEncodingDict behaves like an int32_t.
    '''
    pass
