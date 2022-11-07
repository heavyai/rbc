'''RBC String Dictionary Proxy type
'''

__all__ = ['StringDictionaryProxyNumbaType']


from numba.core import extending, datamodel, cgutils
from numba.core import types as nb_types
from rbc.errors import NumbaTypeError
from rbc.heavydb.buffer import heavydb_buffer_constructor
from rbc.typesystem import Type
from llvmlite import ir


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)


class StringDictionaryProxyNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name='StringDictionaryProxy')


@extending.register_model(StringDictionaryProxyNumbaType)
class StringDictProxyModel(datamodel.models.StructModel):
    def __init__(self, dmm, fe_type):
        members = [
            ('ptr', nb_types.CPointer(nb_types.int8)),
        ]
        super().__init__(dmm, fe_type, members)


extending.make_attribute_wrapper(StringDictionaryProxyNumbaType, 'ptr', 'ptr')


@extending.intrinsic
def proxy_getString_(typingctx, proxy_ptr, string_id):
    def getBytes(builder, ptr, string_id):
        # bytes
        i8p = int8_t.as_pointer()
        fnty = ir.FunctionType(i8p, [i8p, int32_t])
        getStringBytes = cgutils.get_or_insert_function(builder.module, fnty,
                                                        "StringDictionaryProxy_getStringBytes")
        return builder.call(getStringBytes, [ptr, string_id])

    def getBytesLength(context, builder, ptr, string_id):
        # length
        i8p = int8_t.as_pointer()
        size_t = context.get_value_type(Type('size_t')._normalize().tonumba())
        fnty = ir.FunctionType(size_t, [i8p, int32_t])
        getStringLength = cgutils.get_or_insert_function(builder.module, fnty,
                                                         "StringDictionaryProxy_getStringLength")
        return builder.call(getStringLength, [ptr, string_id])

    def codegen(context, builder, signature, args):
        [proxy, string_id] = args
        proxy_ctor = cgutils.create_struct_proxy(signature.args[0])
        proxy = proxy_ctor(context, builder, value=proxy)
        string_id = builder.trunc(string_id, int32_t)

        ptr = getBytes(builder, proxy.ptr, string_id)
        sz = getBytesLength(context, builder, proxy.ptr, string_id)
        text = heavydb_buffer_constructor(context, builder, signature,
                                          [builder.add(sz, sz.type(2))])
        text.sz = sz
        cgutils.memcpy(builder, text.ptr, ptr, sz)
        # string is null terminated
        builder.store(text.ptr.type.pointee(0), builder.gep(text.ptr, [sz]))
        return text._getpointer()

    # importing it here to avoid circular import issue
    from .text_encoding_none import HeavyDBTextEncodingNoneType
    ret = HeavyDBTextEncodingNoneType().tonumba()
    sig = ret(proxy_ptr, string_id)
    return sig, codegen


@extending.intrinsic
def proxy_getStringId_(typingctx, proxy_ptr, str_arg):
    # import here to avoid circular import issue
    from .text_encoding_none import TextEncodingNonePointer

    sig = nb_types.int32(proxy_ptr, str_arg)

    def codegen(context, builder, signature, args):
        [proxy, arg] = args
        if isinstance(str_arg, nb_types.UnicodeType):
            uni_str_ctor = cgutils.create_struct_proxy(nb_types.unicode_type)
            uni_str = uni_str_ctor(context, builder, value=arg)
            c_str = uni_str.data
        elif isinstance(str_arg, TextEncodingNonePointer):
            c_str = builder.extract_value(builder.load(arg), 0)
        else:
            raise NumbaTypeError(f'Cannot handle string argument type {str_arg}')
        i8p = int8_t.as_pointer()
        fnty = ir.FunctionType(int32_t, [i8p, i8p])
        fn = cgutils.get_or_insert_function(builder.module, fnty,
                                            "StringDictionaryProxy_getStringId")
        ret = builder.call(fn, [proxy, c_str])
        return ret

    return sig, codegen


@extending.overload_method(StringDictionaryProxyNumbaType, 'getStringId')
@extending.overload_method(StringDictionaryProxyNumbaType, 'getOrAddTransient')
def ov_getStringId(proxy, str_arg):
    def impl(proxy, str_arg):
        return proxy_getStringId_(proxy.ptr, str_arg)
    return impl


@extending.overload_method(StringDictionaryProxyNumbaType, 'getString')
def heavydb_column_getString(proxy, idx):
    def impl(proxy, idx):
        return proxy_getString_(proxy, idx)
    return impl
