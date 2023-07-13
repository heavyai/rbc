"""Implement Heavydb Column type support

Heavydb Column type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['OutputColumn', 'Column', 'HeavyDBOutputColumnType', 'HeavyDBColumnType',
           'HeavyDBCursorType']

from typing import Union

from llvmlite import ir
from numba.core import extending
from numba.core import types as nb_types

from rbc import typesystem
from rbc.targetinfo import TargetInfo

from . import text_encoding_none
from .buffer import Buffer, BufferPointer, BufferType, HeavyDBBufferType
from .column_list import HeavyDBColumnListType

int8_t = ir.IntType(8)
int32_t = ir.IntType(32)


class HeavyDBColumnType(HeavyDBBufferType):
    """Heavydb Column type for RBC typesystem.
    """

    def _rewire(self):
        # re-wire the implementation of Column<T> to a subtype of Column
        from rbc import heavydb  # is there a better way to do this?
        flatbuffer_columns = ('GeoPoint', 'GeoMultiPoint',
                              'GeoLineString', 'GeoMultiLineString',
                              'GeoPolygon', 'GeoMultiPolygon',
                              'TextEncodingNone')

        for s in flatbuffer_columns:
            # Column<Geo*> | Column<TextEncodingNone>
            geo_cls = getattr(heavydb, f'HeavyDB{s}Type')
            if isinstance(self[0][0], geo_cls):
                p = 'Output' if self.is_output_column else ''
                col_geo_cls = getattr(heavydb, f'HeavyDB{p}Column{s}Type')
                return self.copy(cls=col_geo_cls)

        # Column<Array<T>>
        if isinstance(self[0][0], heavydb.HeavyDBArrayType):
            p = 'Output' if self.is_output_column else ''
            col_arr_cls = getattr(heavydb, f'HeavyDB{p}ColumnArrayType')
            return self.copy(cls=col_arr_cls)

        return self

    def postprocess_type(self):
        return self._rewire().params(shorttypename='Column')

    @property
    def is_output_column(self):
        return False

    def match(self, other):
        if type(self) is type(other):
            return self[0] == other[0]

    @property
    def buffer_extra_members(self):
        heavydb_version = TargetInfo().software[1][:3]
        if heavydb_version >= (6, 2) and self.element_type.tostring() == 'TextEncodingDict':
            return ('i8* string_dict_proxy_',)
        return ()


class HeavyDBOutputColumnType(HeavyDBColumnType):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn is the same as Column but introduced to distinguish
    the input and output arguments of UDTFs.
    """

    def postprocess_type(self):
        return super()._rewire().params(shorttypename='OutputColumn')

    @property
    def is_output_column(self):
        return True


class Column(Buffer):
    """
    RBC ``Column<T>`` type that corresponds to HeavyDB COLUMN

    In HeavyDB, a Column of type ``T`` is represented as follows:

    .. code-block:: c

        {
            T* ptr;
            int64_t sz;
            void* string_dict_proxy;  // available only if T == TextEncodingDict
        }

    """
    def dtype(self):
        """
        Data type of the array elements.
        """

    def get_string_id(self, s: Union[str, 'text_encoding_none.TextEncodingNone']) -> int:  # noqa: E501
        """
        Return the string ID for the given string ``s``.

        .. note::
            Only available on ``CPU`` and ``Column<TextEncodingDict>``
        """

    def get_string(self, id: int) -> int:
        """
        Return the string for the given ``id``.

        .. note::
            Only available on ``CPU`` and ``Column<TextEncodingDict>``
        """

    def __len__(self) -> int:
        """
        """


class OutputColumn(Column):
    pass


@extending.intrinsic
def heavydb_column_set_null_(typingctx, col_var, row_idx):
    # Float values are serialized as integers by HeavyDB
    # For reference, here is the conversion table for float and double
    #   FLOAT:  1.1754944e-38            -> 8388608
    #   DOUBLE: 2.2250738585072014e-308  -> 4503599627370496
    #                    ^                          ^
    #                 fp value                  serialized
    T = col_var.eltype
    sig = nb_types.void(col_var, row_idx)

    target_info = TargetInfo()
    null_value = target_info.null_values[str(T)]

    def codegen(context, builder, signature, args):
        data, index = args
        ptr = builder.extract_value(data, [0])

        ty = ptr.type.pointee
        nv = ir.Constant(ir.IntType(T.bitwidth), null_value)
        if isinstance(T, nb_types.Float):
            nv = builder.bitcast(nv, ty)
        builder.store(nv, builder.gep(ptr, [index]))

    return sig, codegen


@extending.overload_method(BufferType, 'set_null')
def heavydb_column_set_null(col_var, index):
    def impl(col_var, index):
        return heavydb_column_set_null_(col_var, index)
    return impl


@extending.intrinsic
def heavydb_column_is_null_(typingctx, col_var, row_idx):
    T = col_var.eltype
    sig = nb_types.boolean(col_var, row_idx)

    target_info = TargetInfo()
    null_value = target_info.null_values[str(T)]
    nv = ir.Constant(ir.IntType(T.bitwidth), null_value)

    def codegen(context, builder, signature, args):
        data, index = args
        ptr = builder.extract_value(data, [0])
        res = builder.load(builder.gep(ptr, [index]))

        if isinstance(T, nb_types.Float):
            res = builder.bitcast(res, nv.type)

        return builder.icmp_signed('==', res, nv)

    return sig, codegen


@extending.overload_method(BufferType, 'is_null')
def heavydb_column_is_null(col_var, row_idx):
    def impl(col_var, row_idx):
        return heavydb_column_is_null_(col_var, row_idx)
    return impl


@extending.intrinsic
def get_dict_proxy(typingctx, col_var):
    from .string_dict_proxy import StringDictionaryProxyNumbaType
    sig = StringDictionaryProxyNumbaType()(col_var)

    def codegen(context, builder, sig, args):
        [col] = args
        if col.type.is_pointer:
            col = builder.load(col)
        proxy = builder.extract_value(col, [2])
        return proxy

    return sig, codegen


@extending.overload_attribute(BufferType, "string_dict_proxy")
@extending.overload_attribute(BufferPointer, "string_dict_proxy")
def heavydb_column_string_dict_proxy(col_var):
    def impl(col_var):
        return get_dict_proxy(col_var)
    return impl


class HeavyDBCursorType(typesystem.Type):

    @classmethod
    def preprocess_args(cls, args):
        assert len(args) == 1
        params = []
        for p in args[0]:
            if not isinstance(p, (HeavyDBColumnType, HeavyDBColumnListType)):
                # map Cursor<T ...> to Cursor<Column<T> ...>
                c = p.copy()
                p = HeavyDBColumnType((c,), **c._params)
                c._params.clear()
            params.append(p)
        return (tuple(params),)

    @property
    def as_consumed_args(self):
        return self[0]
