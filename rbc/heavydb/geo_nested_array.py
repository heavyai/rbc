"""
Base classes for GEO types
"""

__all__ = [
    "GeoNestedArrayNumbaType",
    "HeavyDBGeoNestedArray",
    "GeoNestedArray",
    "heavydb_geo_fromCoords_vec",
    "heavydb_geo_fromCoords_vec2",
    "heavydb_geo_fromCoords_vec3",
    "heavydb_geo_toCoords_vec",
    "heavydb_geo_toCoords_vec2",
    "heavydb_geo_toCoords_vec3",
]

import operator

from llvmlite import ir
from numba.core import cgutils, extending
from numba.core import types as nb_types

from rbc import typesystem
from rbc.external import external

from .allocator import allocate_varlen_buffer
from .metatype import HeavyDBMetaType
from .utils import as_voidptr, get_alloca

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

    def __len__(self) -> int:
        """
        """

    def size(self) -> int:
        """
        """

    def is_null(self) -> bool:
        """
        Return wether the Geo type is ``NULL``
        """


class GeoNestedArrayNumbaType(nb_types.Type):
    def __init__(self, name):
        super().__init__(name)
        self.base_type = self.__typesystem_type__._params["base_type"]
        self.item_type = self.__typesystem_type__._params["item_type"]


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
            "NumbaType": self.numba_type,
            "name": self.type_name,
            "base_type": self.type_name,
            "item_type": self.item_type,
        }

    def tonumba(self, bool_is_int8=None):
        flatbuffer_t = typesystem.Type.fromstring("int8_t* flatbuffer_")
        index_t = typesystem.Type.fromstring("int64_t index_")
        n_t = typesystem.Type.fromstring("int64_t n_")
        typ = typesystem.Type(
            flatbuffer_t,
            index_t,
            index_t,
            index_t,
            index_t,
            n_t,
        )
        typ.params(other=None, **self.custom_params)
        return typ.tonumba(bool_is_int8=True)


@extending.intrinsic
def get_c_ptr(typingctx, sz, typ):
    t = typ.dtype
    assert isinstance(typ, nb_types.DTypeSpec)
    sig = nb_types.CPointer(t)(sz, typ)

    def codegen(context, builder, sig, args):
        [sz, _] = args
        # TODO: replace i64(8) by i64(sizeof(typ))
        ptr = allocate_varlen_buffer(builder, sz, i64(8))
        typ = sig.args[1].dtype
        llty = context.get_value_type(typ)
        llty_p = llty.as_pointer()
        return builder.bitcast(ptr, llty_p)

    return sig, codegen


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
            context.make_helper(builder, sig.args[0], value=geo)._getpointer(), i8p
        )

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
@extending.overload_method(GeoNestedArrayNumbaType, "size")
def heavydb_geo_len(geo):
    if isinstance(geo, GeoNestedArrayNumbaType):
        base_type = geo.base_type
        size_ = external(f"int64_t {base_type}_size(int8_t*)", devices=['CPU'])

        def impl(geo):
            return size_(as_voidptr(get_alloca(geo)))

        return impl


@extending.overload_method(GeoNestedArrayNumbaType, "is_null")
def heavydb_geo_isNull(geo):
    if isinstance(geo, GeoNestedArrayNumbaType):
        base_type = geo.base_type
        isNull = external(f"bool {base_type}_isNull(int8_t*)", devices=['CPU'])

        def impl(geo):
            return isNull(as_voidptr(get_alloca(geo)))

        return impl


@extending.overload(operator.getitem)
@extending.overload_method(GeoNestedArrayNumbaType, "get_item")
def heavydb_geo_getitem(geo, index):
    if isinstance(geo, GeoNestedArrayNumbaType):

        def impl(geo, index):
            return heavydb_geo_getitem_(geo, index)

        return impl


def heavydb_geo_fromCoords_vec(geo, lst):
    base_type = geo.base_type
    fromCoords = external(
        f"void {base_type}_fromCoords(int8_t*, double*, int64_t size)",
        devices=['CPU']
    )

    def impl(geo, lst):
        sz = len(lst)

        f64p = get_c_ptr(sz, nb_types.double)
        for i in range(sz):
            f64p[i] = lst[i]

        return fromCoords(as_voidptr(get_alloca(geo)), f64p, sz)

    return impl


def heavydb_geo_fromCoords_vec2(geo, lst):
    base_type = geo.base_type
    fromCoords = external(
        f"void {base_type}_fromCoords(int8_t*, double*, int64_t*, int64_t)",
        devices=['CPU']
    )

    def impl(geo, lst):
        dim0 = len(lst)
        geo_ptr = as_voidptr(get_alloca(geo))

        indices = get_c_ptr(dim0, nb_types.int64)
        n_elems = 0
        for i in range(dim0):
            n_elems += len(lst[i])
            indices[i] = len(lst[i])

        data = get_c_ptr(n_elems, nb_types.double)
        idx = 0
        for i in range(dim0):
            dim1 = len(lst[i])
            for j in range(dim1):
                data[idx] = lst[i][j]
                idx += 1

        return fromCoords(geo_ptr, data, indices, dim0)

    return impl


def heavydb_geo_fromCoords_vec3(geo, lst):
    base_type = geo.base_type
    fromCoords = external(
        f"void {base_type}_fromCoords(int8_t*, double*, int64_t*, int64_t*, int64_t, int64_t)",
        devices=['CPU']
    )

    def impl(geo, lst):
        geo_ptr = as_voidptr(get_alloca(geo))

        indices_i = []
        indices_j = []
        flatten = []

        dim0 = len(lst)
        for i in range(dim0):
            cnt = 0
            dim1 = len(lst[i])
            for j in range(dim1):
                dim2 = len(lst[i][j])
                cnt += dim2
                indices_j.append(dim2)
                for k in range(dim2):
                    flatten.append(lst[i][j][k])
            indices_i.append(cnt)

        data = get_c_ptr(len(flatten), nb_types.double)
        for i, e in enumerate(flatten):
            data[i] = e

        indices_i_ = get_c_ptr(len(indices_i), nb_types.int64)
        for i, e in enumerate(indices_i):
            indices_i_[i] = e

        indices_j_ = get_c_ptr(len(indices_j), nb_types.int64)
        for j, e in enumerate(indices_j):
            indices_j_[j] = e

        return fromCoords(
            geo_ptr, data, indices_i_, indices_j_, len(indices_i), len(indices_j)
        )

    return impl


def heavydb_geo_toCoords_vec(geo):
    base_type = geo.base_type
    get_value = external(f"double {base_type}_toCoords_get_value(int8_t*, int64_t)",
                         devices=['CPU'])

    def impl(geo):
        geo_ptr = as_voidptr(get_alloca(geo))

        lst = []
        n_coords = geo.n_coords()
        for i in range(n_coords):
            lst.append(get_value(geo_ptr, i))
        return lst

    return impl


def heavydb_geo_toCoords_vec2(geo):
    base_type = geo.base_type
    get_value = external(
        f"double {base_type}_toCoords_get_value(int8_t*, int64_t, int64_t)",
        devices=['CPU']
    )

    def impl(geo):
        lst = []
        geo_ptr = as_voidptr(get_alloca(geo))
        n_rings = geo.n_rings()
        for i in range(n_rings):
            n_coords = geo[i].n_coords()
            inner = []
            for j in range(n_coords):
                inner.append(get_value(geo_ptr, i, j))
            lst.append(inner)
        return lst

    return impl


def heavydb_geo_toCoords_vec3(geo):
    raise NotImplementedError()
