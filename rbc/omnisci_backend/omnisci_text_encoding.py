'''Omnisci Bytes type that corresponds to Omnisci type TEXT ENCODED NONE.
'''

__all__ = ['OmnisciTextEncodingType', 'TextEncodingDict']

from .omnisci_metatype import OmnisciMetaType
from rbc import typesystem


class OmnisciTextEncodingType(typesystem.Type):
    """Omnisci Text Encoding Dict type for RBC typesystem.
    """

    def tostring(self, use_typename=False, use_annotation=True):
        return f'TextEncodingDict{self.dict_size}'

    @property
    def __typesystem_type__(self):
        return typesystem.Type('int32')


class OmnisciTextEncodingDict8Type(OmnisciTextEncodingType):
    dict_size = 8


class OmnisciTextEncodingDict16Type(OmnisciTextEncodingType):
    dict_size = 16


class OmnisciTextEncodingDict32Type(OmnisciTextEncodingType):
    dict_size = 32


class TextEncodingDict(object, metaclass=OmnisciMetaType):
    pass
