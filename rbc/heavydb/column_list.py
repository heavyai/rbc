__all__ = ['HeavyDBColumnListType', 'ColumnList']


import operator
from numba.core import extending, cgutils, datamodel, imputils

from rbc.heavydb.buffer import Buffer
from numba.core import types as nb_types
from rbc.typesystem import Type
from rbc import structure_type
from rbc.targetinfo import TargetInfo


class HeavyDBColumnListType(Type):

    def postprocess_type(self):
        if self.tostring().startswith('HeavyDBColumnListType<HeavyDBArrayType'):
            from .column_list_array import HeavyDBColumnListArrayType
            return self.copy(cls=HeavyDBColumnListArrayType)
        return self

    @property
    def element_type(self):
        return self[0][0]

    @property
    def buffer_extra_members(self):
        heavydb_version = TargetInfo().software[1][:3]
        if heavydb_version >= (6, 2) and self.element_type.tostring() == 'TextEncodingDict':
            return ('i8** string_dict_proxy_',)
        return ()

    @property
    def numba_pointer_type(self):
        return ColumnListType

    # @property
    # def numba_type(self):
    #     return ColumnList

    @property
    def custom_params(self):
        return {
            # 'NumbaType': self.numba_type,
            'NumbaPointerType': self.numba_pointer_type,
        }

    def tonumba(self, bool_is_int8=None):
        ptrs = self.element_type.pointer().pointer().params(name='ptrs')
        ncols = Type.fromstring('int64 num_cols_')
        nrows = Type.fromstring('int64 num_rows_')
        extra_members = tuple(map(Type.fromobject, self.buffer_extra_members))
        extra_members = tuple(map(Type.fromobject, self.buffer_extra_members))
        column_list_type = Type(
            ptrs,
            ncols,
            nrows,
            *extra_members
        )
        column_list_type.params(other=None, **self.custom_params)
        numba_type = column_list_type.tonumba(bool_is_int8=True)
        return self.numba_pointer_type(numba_type)


@extending.intrinsic
def heavydb_columnlist_getitem(typingctx, lst, idx):
    members = lst.dtype.members
    is_text_encoding_dict = False
    if len(members) == 4:
        is_text_encoding_dict = True
        T = Type.fromstring('TextEncodingDict')
    else:
        T = Type.fromnumba(members[0].dtype.dtype)

    ret = Type.fromstring(f'Column<{T}>').tonumba().dtype
    sig = ret(lst, idx)

    def codegen(context, builder, signature, args):
        [lst, idx] = args
        collist_ctor = cgutils.create_struct_proxy(signature.args[0].dtype)
        collist = collist_ctor(context, builder, value=builder.load(lst))

        col_ctor = cgutils.create_struct_proxy(signature.return_type)
        col = col_ctor(context, builder)

        col.ptr = builder.load(builder.gep(collist.ptrs, [idx]))
        col.sz = collist.num_rows_
        if is_text_encoding_dict:
            col.string_dict_proxy_ = builder.load(builder.gep(collist.string_dict_proxy_, [idx]))
        return col._getvalue()

    return sig, codegen


class ColumnList(Buffer):
    """
    RBC ``ColumnList<T>`` type that corresponds to HeavyDB COLUMN LIST

    In HeavyDB, a ColumnList of type ``T`` is represented as follows:

    .. code-block:: c

        {
            T** ptrs;
            int64_t num_cols_;
            int64_t num_rows_;
        }

    """

    @property
    def nrows(self) -> int:
        """
        Return the number of rows each column has in a ColumnList
        """

    @property
    def ncols(self) -> int:
        """
        Return the number of columns in a ColumnList
        """


class ColumnListType(structure_type.StructureNumbaPointerType, nb_types.IterableType):
    def get_getitem_impl(self):
        def impl(x, i):
            return heavydb_columnlist_getitem(x, i)
        return impl

    @property
    def iterator_type(self):
        return ColumnListIteratorType(self)


class ColumnListIteratorType(nb_types.SimpleIteratorType):

    def __init__(self, buffer_type):
        name = f"iter_buffer({buffer_type})"
        self.buffer_type = buffer_type
        T = buffer_type.dtype.members[0].dtype.dtype
        yield_type = Type.fromstring(f'Column<{T}>').tonumba().dtype
        super().__init__(name, yield_type)


@datamodel.register_default(ColumnListIteratorType)
class BufferPointerIteratorModel(datamodel.StructModel):
    def __init__(self, dmm, fe_type):
        members = [('index', nb_types.EphemeralPointer(nb_types.uintp)),
                   ('buffer', fe_type.buffer_type)]
        super(BufferPointerIteratorModel, self).__init__(dmm, fe_type, members)


@extending.overload_attribute(ColumnListType, 'nrows')
def get_nrows(clst):
    def impl(clst):
        return clst.num_rows_
    return impl


@extending.overload_attribute(ColumnListType, 'ncols')
def get_ncols(clst):
    def impl(clst):
        return clst.num_cols_
    return impl


@extending.lower_builtin('iternext', ColumnListIteratorType)
@imputils.iternext_impl(imputils.RefType.UNTRACKED)
def iternext_BufferPointer(context, builder, sig, args, result):
    [iterbufty] = sig.args
    [bufiter] = args

    iterval = context.make_helper(builder, iterbufty, value=bufiter)

    buf = iterval.buffer
    idx = builder.load(iterval.index)

    lst = context.make_helper(builder, iterbufty.buffer_type.dtype,
                              value=builder.load(buf))
    count = lst.num_cols_

    is_valid = builder.icmp_signed('<', idx, count)
    result.set_valid(is_valid)

    with builder.if_then(is_valid):
        getitem_fn = context.typing_context.resolve_value_type(operator.getitem)
        getitem_sig = iterbufty.yield_type(iterbufty.buffer_type, nb_types.intp)
        getitem_fn.get_call_type(context.typing_context, getitem_sig.args, {})
        getitem_out = context.get_function(getitem_fn, getitem_sig)(builder, [buf, idx])
        result.yield_(getitem_out)
        nidx = builder.add(idx, context.get_constant(nb_types.intp, 1))
        builder.store(nidx, iterval.index)


@extending.lower_builtin('getiter', ColumnListType)
def getiter_buffer_pointer(context, builder, sig, args):
    [buffer] = args

    iterobj = context.make_helper(builder, sig.return_type)

    # set the index to zero
    zero = context.get_constant(nb_types.uintp, 0)
    indexptr = cgutils.alloca_once_value(builder, zero)

    iterobj.index = indexptr

    # wire in the buffer type data
    iterobj.buffer = buffer

    res = iterobj._getvalue()
    return imputils.impl_ret_new_ref(context, builder, sig.return_type, res)
