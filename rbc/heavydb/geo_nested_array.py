"""
Base classes for GEO types
"""

__all__ = ["GeoNestedArrayNumbaType", "HeavyDBGeoNestedArray", "GeoNestedArray"]

import operator

from llvmlite import ir

from numba.core import types as nb_types
from numba.core import extending, cgutils

from rbc.external import external
from rbc import typesystem

from .utils import as_voidptr, get_alloca
from .metatype import HeavyDBMetaType

i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()


class GeoNestedArray(metaclass=HeavyDBMetaType):
    """
    Base class for GEO types.

    .. note::
        Geo columns should inherit from ``ColumnFlatBuffer`` in
        ``column_flatbuffer.py``.

    .. code-block:: c

        {
            int8_t* flatbuffer_;
            int64_t index_[4];
            int64_t n_;
        }
    """
    pass


class GeoNestedArrayNumbaType(nb_types.Type):
    def __init__(self, name):
        super().__init__(name)
        self.base_type = self.__typesystem_type__._params['base_type']
        self.item_type = self.__typesystem_type__._params['item_type']


class HeavyDBGeoNestedArray(typesystem.Type):
    """Typesystem type class for HeavyDB buffer structures."""

    @property
    def numba_type(self):
        return GeoNestedArrayNumbaType

    @property
    def type_name(self):
        raise NotImplementedError()

    @property
    def item_type(self):
        raise NotImplementedError()

    def postprocess_type(self):
        return self.params(shorttypename=self.type_name)

    @property
    def custom_params(self):
        return {
            'NumbaType': self.numba_type,
            "name": self.type_name,
            "base_type": self.type_name,
            "item_type": self.item_type,
        }

    def tonumba(self, bool_is_int8=None):
        flatbuffer_t = typesystem.Type.fromstring("int8_t* flatbuffer_")
        index_t = typesystem.Type.fromstring("int64_t index_t")
        n_t = typesystem.Type.fromstring("int64_t n_")
        geoline_type = typesystem.Type(
            flatbuffer_t,
            # int64_t index[4]
            index_t,
            index_t,
            index_t,
            index_t,
            n_t,
        )
        geoline_type.params(other=None, **self.custom_params)
        return geoline_type.tonumba(bool_is_int8=True)


@extending.intrinsic
def heavydb_geo_getitem_(typingctx, geo, index):
    retty = typesystem.Type.fromstring(geo.item_type).tonumba()
    sig = retty(geo, index)

    def codegen(context, builder, sig, args):
        geo, index = args
        fnty = ir.FunctionType(void, [i8p, i64, i8p])
        getItem = cgutils.get_or_insert_function(
            builder.module, fnty, f"{sig.args[0].base_type}_getItem"
        )
        geo_ptr = builder.bitcast(
            context.make_helper(builder, sig.args[0], value=geo)._getpointer(),
            i8p)

        # Alloca ItemType
        fa = context.make_helper(builder, sig.return_type)

        # ItemType -> void*
        result_ptr = builder.bitcast(fa._getpointer(), i8p)

        # call func
        builder.call(getItem, [geo_ptr, index, result_ptr])

        # convert void* -> ItemType
        point_type = fa._get_be_type(fa._datamodel)
        return builder.load(builder.bitcast(result_ptr, point_type.as_pointer()))

    return sig, codegen


@extending.overload(len)
@extending.overload_method(GeoNestedArrayNumbaType, 'size')
def heavydb_geo_len(geo_nested_array):
    base_type = geo_nested_array.base_type
    size_ = external(f"int64_t {base_type}_size(int8_t*)|cpu")

    if isinstance(geo_nested_array, GeoNestedArrayNumbaType):
        def impl(geo_nested_array):
            return size_(as_voidptr(get_alloca(geo_nested_array)))
        return impl


@extending.overload_method(GeoNestedArrayNumbaType, 'is_null')
def heavydb_geo_isNull(geo_nested_array):
    base_type = geo_nested_array.base_type
    isNull = external(f"bool {base_type}_isNull(int8_t*)|cpu")

    if isinstance(geo_nested_array, GeoNestedArrayNumbaType):
        def impl(geo_nested_array):
            return isNull(as_voidptr(get_alloca(geo_nested_array)))
        return impl


@extending.overload(operator.getitem)
@extending.overload_method(GeoNestedArrayNumbaType, "get_item")
def heavydb_geo_getitem(geo, index):
    def impl(geo, index):
        return heavydb_geo_getitem_(geo, index)
    return impl
