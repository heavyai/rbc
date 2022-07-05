__all__ = ['HeavyDBColumnListType', 'ColumnList']


from numba.core import extending, cgutils
from rbc.heavydb.buffer import Buffer
from rbc.typesystem import Type
from rbc import structure_type


class HeavyDBColumnListType(Type):

    @property
    def element_type(self):
        return self[0][0]

    @property
    def buffer_extra_members(self):
        if self.element_type.tostring() == 'TextEncodingDict':
            return ('i8** string_dict_proxy_',)
        return ()

    @property
    def __typesystem_type__(self):
        ptrs_t = self.element_type.pointer().pointer().params(name='ptrs')
        length_t = Type.fromstring('int64 length')
        size_t = Type.fromstring('int64 size')
        extra_members = tuple(map(Type.fromobject, self.buffer_extra_members))
        return Type(ptrs_t, length_t, size_t, *extra_members).params(
            NumbaPointerType=HeavyDBColumnListNumbaType).pointer()


@extending.intrinsic
def heavydb_columnlist_getitem(typingctx, lst, idx):
    members = lst.dtype.members
    is_text_encoding_dict = False
    if len(members) == 4:
        is_text_encoding_dict = True
        T = Type.fromstring('TextEncodingDict')
    else:
        T = Type.fromnumba(lst.dtype.members[0].dtype.dtype)

    ret = Type.fromstring(f'Column<{T}>').tonumba().dtype
    sig = ret(lst, idx)

    def codegen(context, builder, signature, args):
        [lst, idx] = args
        collist_ctor = cgutils.create_struct_proxy(signature.args[0].dtype)
        collist = collist_ctor(context, builder, value=builder.load(lst))

        col_ctor = cgutils.create_struct_proxy(signature.return_type)
        col = col_ctor(context, builder)

        col.ptr = builder.load(builder.gep(collist.ptrs, [idx]))
        col.sz = collist.size
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
            int64_t nrows;
            int64_t ncols;
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


class HeavyDBColumnListNumbaType(structure_type.StructureNumbaPointerType):
    def get_getitem_impl(self):
        def impl(x, i):
            return heavydb_columnlist_getitem(x, i)
        return impl


@extending.overload_attribute(HeavyDBColumnListNumbaType, 'nrows')
def get_nrows(clst):
    def impl(clst):
        return clst.size
    return impl


@extending.overload_attribute(HeavyDBColumnListNumbaType, 'ncols')
def get_ncols(clst):
    def impl(clst):
        return clst.length
    return impl
