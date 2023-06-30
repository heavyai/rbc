'''RBC String Dictionary Proxy type
'''

__all__ = ['StringDictionaryProxyNumbaType', 'StringDictionaryProxy']


from llvmlite import ir
from numba.core import extending
from numba.core import types as nb_types
from numba.core.pythonapi import PY_UNICODE_1BYTE_KIND
from numba.cpython.unicode import _empty_string, _set_code_point

from rbc.errors import NumbaTypeError
from rbc.external import external

from . import text_encoding_none
from .metatype import HeavyDBMetaType
from .opaque_pointer import HeavyDBOpaquePtr, OpaquePtrNumbaType
from .utils import as_voidptr

int8_t = ir.IntType(8)
int32_t = ir.IntType(32)


class StringDictionaryProxy(metaclass=HeavyDBMetaType):

    def getStringId(self, str_arg: str) -> int:
        """
        """

    def getOrAddTransient(self, str_arg: str) -> int:
        """
        """

    def getString(self, index: int) -> str:
        """
        """


class HeavyDBStringDictProxyType(HeavyDBOpaquePtr):
    """RowFunctionManager<> is a typesystem custom type that
    represents a class type with the following public interface:

      struct RowFunctionManager { }
    """

    @property
    def numba_type(self):
        return StringDictionaryProxyNumbaType

    @property
    def type_name(self):
        return "StringDictionaryProxy"


class StringDictionaryProxyNumbaType(OpaquePtrNumbaType):
    pass


HeavyDBStringDictProxyType().register_datamodel()


@extending.overload_method(StringDictionaryProxyNumbaType, 'getStringId')
@extending.overload_method(StringDictionaryProxyNumbaType, 'getOrAddTransient')
def ov_getStringId(proxy, str_arg):
    getStringId_ = external("int32 StringDictionaryProxy_getStringId(int8*, int8*)")

    if isinstance(str_arg, nb_types.UnicodeType):
        def impl(proxy, str_arg):
            return getStringId_(as_voidptr(proxy), as_voidptr(str_arg._data))
        return impl
    elif isinstance(str_arg, text_encoding_none.TextEncodingNonePointer):
        def impl(proxy, str_arg):
            return getStringId_(as_voidptr(proxy), as_voidptr(str_arg.ptr))
        return impl
    else:
        raise NumbaTypeError(f'Cannot handle string argument type {str_arg}')


@extending.overload_method(StringDictionaryProxyNumbaType, 'getString')
def heavydb_column_getString(proxy, string_id):
    getBytes = external('int8* StringDictionaryProxy_getStringBytes(int8*, int32)')
    getBytesLength = external('int64 StringDictionaryProxy_getStringLength(int8*, int32)')

    def impl(proxy, string_id):
        ptr = getBytes(as_voidptr(proxy), string_id)
        length = getBytesLength(as_voidptr(proxy), string_id)
        text = text_encoding_none.TextEncodingNone(ptr, length)
        kind = PY_UNICODE_1BYTE_KIND  # ASCII characters only
        is_ascii = True
        s = _empty_string(kind, length, is_ascii)
        for i in range(length):
            ch = text[i]
            _set_code_point(s, i, ch)
        return s
    return impl
