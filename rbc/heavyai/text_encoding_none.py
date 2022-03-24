'''Omnisci TextEncodingNone type that corresponds to Omnisci type TEXT ENCODED NONE.
'''

__all__ = ['TextEncodingNonePointer', 'TextEncodingNone', 'OmnisciTextEncodingNoneType']

from rbc import typesystem
from .buffer import (
    BufferPointer, Buffer, OmnisciBufferType,
    omnisci_buffer_constructor)
from numba.core import types, extending


class OmnisciTextEncodingNoneType(OmnisciBufferType):
    """Omnisci TextEncodingNone type for RBC typesystem.
    """

    @classmethod
    def preprocess_args(cls, args):
        element_type = typesystem.Type.fromstring('char8')
        return ((element_type,),)

    @property
    def buffer_extra_members(self):
        return ('bool is_null',)

    def match(self, other):
        if type(self) is type(other):
            return self[0] == other[0]
        if other.is_pointer and other[0].is_char and other[0].bits == 8:
            return 1
        if other.is_string:
            return 2


TextEncodingNonePointer = BufferPointer


class TextEncodingNone(Buffer):
    '''Omnisci TextEncodingNone type that corresponds to Omnisci type TEXT ENCODED NONE.

    Omnisci TextEncodingNone represents the following structure:

    struct TextEncodingNone {
        char* ptr;
        size_t sz;  // when non-negative, TextEncodingNone has fixed width.
    }


    .. code-block:: python

        from rbc.omnisci_backend import TextEncodingNone

        @omnisci('TextEncodingNone(int32, int32)')
        def make_abc(first, n):
            r = TextEncodingNone(n)
            for i in range(n):
                r[i] = first + i
            return r
    '''
    pass


@extending.lower_builtin(TextEncodingNone, types.Integer)
def omnisci_text_encoding_none_constructor(context, builder, sig, args):
    return omnisci_buffer_constructor(context, builder, sig, args)


@extending.type_callable(TextEncodingNone)
def type_omnisci_text_encoding_none(context):
    def typer(size):
        return typesystem.Type.fromobject('TextEncodingNone').tonumba()
    return typer
