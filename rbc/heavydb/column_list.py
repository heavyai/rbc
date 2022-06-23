__all__ = ['HeavyDBColumnListType', 'ColumnList']


from numba.core import extending
from rbc.heavydb.buffer import Buffer
from rbc.typesystem import Type
from rbc import structure_type


class HeavyDBColumnListType(Type):

    @property
    def __typesystem_type__(self):
        element_type = self[0][0]
        ptrs_t = element_type.pointer().pointer().params(name='ptrs')
        length_t = Type.fromstring('int64 length')
        size_t = Type.fromstring('int64 size')
        return Type(ptrs_t, length_t, size_t).params(
            NumbaPointerType=HeavyDBColumnListNumbaType).pointer()


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
            return x.ptrs[i]
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
