__all__ = ['HeavyDBColumnListType']


from numba.core import extending
from rbc.typesystem import Type
from rbc import structure_type


class HeavyDBColumnListType(Type):
    """HeavyDBColumnListType<T> is a typesystem custom type that
    represents a pointer type of the following struct type:

      {
        T** ptrs
        int64_t length
        int64_t size
      }

    """

    @property
    def __typesystem_type__(self):
        element_type = self[0][0]
        ptrs_t = element_type.pointer().pointer().params(name='ptrs')
        length_t = Type.fromstring('int64 length')
        size_t = Type.fromstring('int64 size')
        return Type(ptrs_t, length_t, size_t).params(
            NumbaPointerType=HeavyDBColumnListNumbaType).pointer()


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
