"""Implement Omnisci Array type support
"""

__all__ = ['ArrayPointer', 'Array', 'omnisci_array_constructor',
           'array_type_converter']

import warnings
from collections import defaultdict
from llvmlite import ir
from rbc import typesystem
from rbc.utils import get_version
from .omnisci_buffer import (BufferPointer, Buffer, BufferPointerModel,
                             buffer_type_converter, OmnisciBufferType)

if get_version('numba') >= (0, 49):
    from numba.core import datamodel, cgutils, extending, types
else:
    from numba import datamodel, cgutils, extending, types


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()


class OmnisciArrayType(OmnisciBufferType):
    """Omnisci Array type for RBC typesystem.
    """


class ArrayPointer(BufferPointer):
    """Type class for pointers to :code:`Omnisci Array<T>` structure."""


class Array(Buffer):
    pass


@datamodel.register_default(ArrayPointer)
class ArrayPointerModel(BufferPointerModel):
    pass


builder_buffers = defaultdict(list)


@extending.lower_builtin(Array, types.Integer, types.StringLiteral)
@extending.lower_builtin(Array, types.Integer, types.NumberClass)
def omnisci_array_constructor(context, builder, sig, args):
    if not context.target_info.is_cpu:
        warnings.warn(
            f'allocating arrays in {context.target_info.name}'
            ' is not supported')
    ptr_type, sz_type, null_type = sig.return_type.dtype.members

    # zero-extend the element count to int64_t
    assert isinstance(args[0].type, ir.IntType), (args[0].type)
    element_count = builder.zext(args[0], int64_t)
    element_size = int64_t(ptr_type.dtype.bitwidth // 8)

    '''
    QueryEngine/ArrayOps.cpp:
    int8_t* allocate_varlen_buffer(int64_t element_count, int64_t element_size)
    '''
    alloc_fnty = ir.FunctionType(int8_t.as_pointer(), [int64_t, int64_t])
    # see https://github.com/xnd-project/rbc/issues/75
    alloc_fn = builder.module.get_or_insert_function(
        alloc_fnty, name="calloc")
    ptr8 = builder.call(alloc_fn, [element_count, element_size])
    builder_buffers[builder].append(ptr8)
    ptr = builder.bitcast(ptr8, context.get_value_type(ptr_type))
    is_null = context.get_value_type(null_type)(0)

    # construct array
    fa = cgutils.create_struct_proxy(sig.return_type.dtype)(context, builder)
    fa.ptr = ptr              # T*
    fa.sz = element_count     # size_t
    fa.is_null = is_null      # int8_t
    return fa._getpointer()


@extending.type_callable(Array)
def type_omnisci_array(context):
    def typer(size, dtype):
        if isinstance(dtype, types.StringLiteral):
            typ = 'Array<{}>'.format(dtype.literal_value)
        elif isinstance(dtype, types.NumberClass):
            typ = 'Array<{}>'.format(dtype.dtype)
        else:
            raise NotImplementedError(repr(dtype))
        atyp = array_type_converter(context.target_info, typ)
        if atyp is not None:
            return atyp.tonumba()
        raise NotImplementedError((dtype, typ))
    return typer


@extending.intrinsic
def omnisci_array_is_null_(typingctx, data):
    sig = types.int8(data)

    def codegen(context, builder, signature, args):

        rawptr = cgutils.alloca_once_value(builder, value=args[0])
        ptr = builder.load(rawptr)

        return builder.load(builder.gep(ptr, [int32_t(0), int32_t(2)]))

    return sig, codegen


@extending.overload_method(ArrayPointer, 'is_null')
def omnisci_array_is_null(x):
    if isinstance(x, ArrayPointer):
        def impl(x):
            return omnisci_array_is_null_(x)
        return impl


def array_type_converter(target_info, obj):
    """Return Type instance corresponding to :code:`Omnisci Array<T>` type.

    :code:`Omnisci Array<T>` is defined as follows (using C++ syntax)::

      template<typename T>
      struct Array {
        T* ptr;
        size_t sz;
        bool is_null;
      }

    See :code:`buffer_type_converter` for details.
    """
    buffer_type = buffer_type_converter(
        target_info, obj, OmnisciArrayType,
        'Array', ArrayPointer,
        extra_members=[
            typesystem.Type.fromstring('bool is_null',
                                       target_info=target_info)])
    if buffer_type is not None:
        return buffer_type.pointer()
    return buffer_type
