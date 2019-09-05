import re
import operator
import numba
from llvmlite import ir
from . import typesystem


class ArrayPointer(numba.types.Type):
    """Type class for pointers to Omnisci Array<T> structure.

    We are not deriving from CPointer because ArrayPointer getitem is
    used to access the data stored in Array ptr member.
    """
    mutable = True

    def __init__(self, dtype):
        self.dtype = dtype
        name = "(%s)*" % dtype
        super(ArrayPointer, self).__init__(name)

    @property
    def key(self):
        return self.dtype


@numba.datamodel.register_default(ArrayPointer)
class ArrayPointerModel(numba.datamodel.models.PointerModel):
    pass


@numba.extending.intrinsic
def mapd_array_len_(typingctx, data):
    sig = numba.types.int64(data)

    def codegen(context, builder, signature, args):
        data, = args
        rawptr = numba.cgutils.alloca_once_value(builder, value=data)
        struct = builder.load(builder.gep(rawptr,
                                          [ir.Constant(ir.IntType(32), 0)]))
        return builder.load(builder.gep(
            struct, [ir.Constant(ir.IntType(32), 0),
                     ir.Constant(ir.IntType(32), 1)]))
    return sig, codegen


@numba.extending.overload(len)
def mapd_array_len(x):
    if isinstance(x, ArrayPointer):
        return lambda x: mapd_array_len_(x)


@numba.extending.intrinsic
def mapd_array_getitem_(typingctx, data, index):
    sig = numba.types.int32(data, index)

    def codegen(context, builder, signature, args):
        data, index = args
        rawptr = numba.cgutils.alloca_once_value(builder, value=data)
        struct = builder.load(builder.gep(rawptr,
                                          [ir.Constant(ir.IntType(32), 0)]))
        ptr = builder.load(builder.gep(
            struct, [ir.Constant(ir.IntType(32), 0),
                     ir.Constant(ir.IntType(32), 0)]))
        return builder.load(builder.gep(ptr, [index]))
    return sig, codegen


@numba.extending.overload(operator.getitem)
def mapd_array_getitem(x, i):
    if isinstance(x, ArrayPointer):
        def impl(x, i):
            return mapd_array_getitem_(x, i)
        return impl


_array_type_match = re.compile(r'\A(.*)\s*[[]\s*[]]\Z').match


def array_type_converter(target_info, obj):
    """Return Type instance corresponding to Omniscidb `Array` type.

    Omniscidb `Array` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Array {
        T* ptr;
        size_t sz;
        bool is_null;
      }

    Parameters
    ----------
    obj : str
      Specify a string in the form `T[]` where `T` specifies the Array
      items type.
    """
    if isinstance(obj, str):
        m = _array_type_match(obj)
        if m is not None:
            t = typesystem.Type.fromstring(m.group(1), target_info=target_info)
            array_type = typesystem.Type(
                typesystem.Type(t, '*', name='ptr'),
                typesystem.Type.fromstring('size_t sz',
                                           target_info=target_info),
                typesystem.Type.fromstring('byte is_null',
                                           target_info=target_info),
            )
            array_type_ptr = array_type.pointer()
            numba_type_ptr = ArrayPointer(array_type.tonumba())

            array_type_ptr._params['typename'] = 'Array<%s_t>' % (t)
            array_type_ptr._params['tonumba'] = numba_type_ptr

            return array_type_ptr
