"""Implement Heavydb Column<TextEncodingNone> type support

Heavydb Column<TextEncodingNone> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = [
    "HeavyDBOutputColumnTextEncodingNoneType",
    "HeavyDBColumnTextEncodingNoneType",
    "ColumnTextEncodingNone",
]

import operator

from llvmlite import ir
from numba.core import extending, cgutils

from rbc import typesystem

from .column_flatbuffer import (ColumnFlatBuffer, ColumnFlatBufferPointer,
                                ColumnFlatBufferType,
                                HeavyDBColumnFlatBufferType,
                                HeavyDBOutputColumnFlatBufferType)
from .text_encoding_none import TextEncodingNone

i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()


class ColumnTextEncodingNone(ColumnFlatBuffer):
    """
    RBC ``Column<TextEncodingNone>`` type that corresponds to HeavyDB
    ``COLUMN<TEXT_ENCODING_NONE>``

    In HeavyDB, a Column of type ``TextEncodingNone`` is represented as follows:

    .. code-block:: c

        {
            int8_t* flatbuffer;
            int64_t size;
        }

    """

    def __getitem__(self, index: int) -> "TextEncodingNone.TextEncodingNone":
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def get_item(self, index: int) -> "TextEncodingNone.TextEncodingNone":
        """
        Return ``self[index]``

        .. note::
            Only available on ``CPU``
        """

    def set_item(self, index: int, buf: "TextEncodingNone.TextEncodingNone") -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """

    def __setitem__(self, index: int, buf: "TextEncodingNone.TextEncodingNone") -> None:
        """
        Set line from a buffer of point coordindates

        .. note::
            Only available on ``CPU``
        """


class HeavyDBColumnTextEncodingNoneType(HeavyDBColumnFlatBufferType):
    """ """

    @property
    def numba_type(self):
        return ColumnTextEncodingNoneType

    @property
    def numba_pointer_type(self):
        return ColumnTextEncodingNonePointer


    @property
    def type_name(self):
        return "TextEncodingNone"


class HeavyDBOutputColumnTextEncodingNoneType(
    HeavyDBColumnTextEncodingNoneType, HeavyDBOutputColumnFlatBufferType
):
    """Heavydb OutputColumn type for RBC typesystem.

    OutputColumn<TextEncodingNone> is the same as Column<TextEncodingNone> but introduced to
    distinguish the input and output arguments of UDTFs.
    """


class ColumnTextEncodingNoneType(ColumnFlatBufferType):
    pass


class ColumnTextEncodingNonePointer(ColumnFlatBufferPointer):
    pass


@extending.intrinsic
def heavydb_column_getitem_(typingctx, col, index):
    text = typesystem.Type.fromstring("TextEncodingNone").tonumba()
    # oddly enough, a Column<GeoPoint> returns a Point2D
    sig = text(col, index)

    def codegen(context, builder, sig, args):
        col, index = args
        fnty = ir.FunctionType(void, [i8p, i64, i32, i8p])
        getItem = cgutils.get_or_insert_function(
            builder.module, fnty, "ColumnTextEncodingNone_getItem"
        )
        output_srid = i32(0)
        col_ptr = builder.bitcast(col, i8p)

        # Alloca TextEncodingNone
        fa = context.make_helper(builder, sig.return_type.dtype)

        # Point2D -> void*
        result_ptr = builder.bitcast(fa._getpointer(), i8p)

        # call func
        builder.call(getItem, [col_ptr, index, output_srid, result_ptr])

        # convert void* -> Point2D
        point_type = fa._get_be_type(fa._datamodel)
        return builder.bitcast(result_ptr, point_type.as_pointer())

    return sig, codegen


@extending.overload(operator.getitem)
@extending.overload_method(ColumnTextEncodingNonePointer, "get_item")
def heavydb_column_getitem(col, index):
    if isinstance(col, ColumnTextEncodingNonePointer):

        def impl(col, index):
            return heavydb_column_getitem_(col, index)

        return impl
