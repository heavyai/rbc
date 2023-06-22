"""RBC FlatBuffer_TextEncodingNone type that corresponds to HeavyDB type
flatbuffer::TextEncodingNone.
"""

__all__ = ["HeavyDBFlatBufferTextEncodingNoneType", "FlatBuffer_TextEncodingNone"]

from .geo_nested_array import (GeoNestedArray, GeoNestedArrayNumbaType,
                               HeavyDBGeoNestedArray)


class FlatBuffer_TextEncodingNoneNumbaType(GeoNestedArrayNumbaType):
    def __init__(self, name):
        super().__init__(name)


class HeavyDBFlatBufferTextEncodingNoneType(HeavyDBGeoNestedArray):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return FlatBuffer_TextEncodingNoneNumbaType

    @property
    def type_name(self):
        return "flatbuffer_TextEncodingNone"

    @property
    def item_type(self):
        return "char8"


class FlatBuffer_TextEncodingNone(GeoNestedArray):
    """
    RBC type that corresponds to HeavyDB type ``TextEncodingNone``

    .. code-block:: c

        struct TextEncodingNone {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
