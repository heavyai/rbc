__all__ = ['OmnisciColumnListType']


from rbc.typesystem import Type
from rbc import structure_type

lowering_registry = structure_type.lowering_registry
typing_registry = structure_type.typing_registry


class OmnisciColumnListType(Type):
    """OmnisciColumnListType<T> is a typesystem custom type that
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
            NumbaPointerType=OmnisciColumnListNumbaType).pointer()


class OmnisciColumnListNumbaType(structure_type.StructureNumbaPointerType):
    def get_getitem_impl(self):
        def impl(x, i):
            return x.ptrs[i]
        return impl
