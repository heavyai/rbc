
"""Implement Heavydb Column<Array<T>> type support

Heavydb Column<Array<T>> type is the type of input/output column arguments in
UDTFs.
"""

__all__ = ['HeavyDBColumnListArrayType']

import operator
from rbc import typesystem
from .column_list import HeavyDBColumnListType
from numba.core import extending, cgutils
from numba.core import types as nb_types
from llvmlite import ir


i1 = ir.IntType(1)
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i64 = ir.IntType(64)
i64p = i64.as_pointer()
void = ir.VoidType()


_COLUMN_PARAM_NAME = 'ColumnListArray_inner_type'


class HeavyDBColumnListArrayType(HeavyDBColumnListType):

    def postprocess_type(self):
        return self

    @property
    def numba_type(self):
        return ColumnListArrayType

    @property
    def numba_pointer_type(self):
        return ColumnListArrayPointer

    @property
    def element_type(self):
        return typesystem.Type.fromstring('int8_t')

    @property
    def custom_params(self):
        return {
            'NumbaType': self.numba_type,
            'NumbaPointerType': self.numba_pointer_type,
            _COLUMN_PARAM_NAME: self[0][0]
        }


class ColumnListArrayType(nb_types.Type):
    """Numba type class for HeavyDB ColumnList<Array<T>> structures.
    """

    def __init__(self, dtype):
        array_T = self.__typesystem_type__._params.get(_COLUMN_PARAM_NAME)
        eltype = array_T.tonumba().eltype
        self.dtype = array_T.tonumba()  # struct dtype
        name = f"ColumnList<Array<{eltype}>>"
        super().__init__(name)

    @property
    def eltype(self):
        """
        Return buffer element dtype.
        """
        return self.dtype.eltype


class ColumnListArrayPointer(nb_types.Type):
    """Numba type class for pointers to HeavyDB buffer structures.

    We are not deriving from CPointer because ColumnListArrayPointer getitem is
    used to access the data stored in Buffer ptr member.
    """
    mutable = True
    return_as_first_argument = True

    def __init__(self, dtype):
        self.dtype = dtype    # struct dtype
        name = "%s[%s]*" % (type(self).__name__, dtype)
        super().__init__(name)

    @property
    def key(self):
        return self.dtype


@extending.intrinsic
def heavydb_column_list_array_nrows_(typingctx, x):
    sig = nb_types.int64(x)

    def codegen(context, builder, sig, args):
        [col] = args
        proxy = cgutils.create_struct_proxy(sig.args[0].dtype)
        clst = proxy(context, builder, value=builder.load(col))
        return clst.num_rows_

    return sig, codegen


@extending.intrinsic
def heavydb_column_list_array_ncols_(typingctx, x):
    sig = nb_types.int64(x)

    def codegen(context, builder, sig, args):
        [col] = args
        proxy = cgutils.create_struct_proxy(sig.args[0].dtype)
        clst = proxy(context, builder, value=builder.load(col))
        return clst.num_cols_

    return sig, codegen


@extending.intrinsic
def heavydb_column_list_array_getitem_(typingctx, x, i):
    T = x.dtype.eltype
    col = typesystem.Type.fromstring(f'Column<Array<{T}>>').tonumba().dtype
    sig = col(x, i)

    def codegen(context, builder, sig, args):
        ptr, index = args
        clst = cgutils.create_struct_proxy(sig.args[0].dtype)(
            context, builder, value=builder.load(ptr))
        col = cgutils.create_struct_proxy(sig.return_type)(context, builder)
        col.ptr = builder.load(builder.gep(clst.ptrs, [index]))
        col.sz = clst.num_rows_
        return col._getvalue()

    return sig, codegen


@extending.overload(operator.getitem)
def heavydb_column_list_array_getitem(x, i):
    if isinstance(x, ColumnListArrayPointer):
        def impl(x, i):
            return heavydb_column_list_array_getitem_(x, i)
        return impl


@extending.overload_attribute(ColumnListArrayPointer, 'nrows')
def get_nrows(clst):
    def impl(clst):
        return heavydb_column_list_array_nrows_(clst)
    return impl


@extending.overload_attribute(ColumnListArrayPointer, 'ncols')
def get_ncols(clst):
    def impl(clst):
        return heavydb_column_list_array_ncols_(clst)
    return impl
