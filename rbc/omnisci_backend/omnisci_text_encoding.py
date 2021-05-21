'''Omnisci Bytes type that corresponds to Omnisci type TEXT ENCODED NONE.
'''

__all__ = ['OmnisciTextEncodingDictType', 'TextEncodingDict']

from .omnisci_metatype import OmnisciMetaType
from rbc import typesystem


class OmnisciTextEncodingDictType(typesystem.Type):
    """Omnisci Text Encoding Dict type for RBC typesystem.
    """

    def tostring(self, use_typename=False, use_annotation=True):
        return 'TextEncodingDict'

    @property
    def __typesystem_type__(self):
        return typesystem.Type('int32')


class TextEncodingDict(object, metaclass=OmnisciMetaType):
    pass
