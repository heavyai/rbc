'''RBC String Dictionary Proxy type
'''

__all__ = ['StringDictionaryProxyNumbaType', 'StringDictionaryProxy']


from llvmlite import ir
from numba.core import extending
from numba.core import types as nb_types

from rbc.external import external
from rbc.errors import NumbaTypeError

from . import text_encoding_none
from .metatype import HeavyDBMetaType
from .opaque_pointer import HeavyDBOpaquePtr, OpaquePtrNumbaType
from .utils import as_voidptr

int8_t = ir.IntType(8)
int32_t = ir.IntType(32)


class StringDictionaryProxy(metaclass=HeavyDBMetaType):

    def getStringId(self, str_arg: 'text_encoding_none.TextEncodingNone') -> int:
        """
        """

    def getOrAddTransient(self, str_arg: 'text_encoding_none.TextEncodingNone') -> int:
        """
        """

    def getString(self, index: int) -> 'text_encoding_none.TextEncodingNone':
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

    @property
    def supported_devices(self):
        return {'cpu'}


class StringDictionaryProxyNumbaType(OpaquePtrNumbaType):
    pass


HeavyDBStringDictProxyType().register_datamodel()


@extending.overload_method(StringDictionaryProxyNumbaType, 'getStringId')
@extending.overload_method(StringDictionaryProxyNumbaType, 'getOrAddTransient')
def ov_getStringId(proxy, str_arg):
    getStringId_ = external("int32 StringDictionaryProxy_getStringId(int8*, int8*)|CPU")

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
    getBytes = external('int8* StringDictionaryProxy_getStringBytes(int8*, int32)|CPU')
    getBytesLength = external('int64 StringDictionaryProxy_getStringLength(int8*, int32)|CPU')

    def impl(proxy, string_id):
        ptr = getBytes(as_voidptr(proxy), string_id)
        len = getBytesLength(as_voidptr(proxy), string_id)
        return text_encoding_none.TextEncodingNone(ptr, len)
    return impl
