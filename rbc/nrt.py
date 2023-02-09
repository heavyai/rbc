from numba.core import cgutils
from llvmlite import ir

void = ir.VoidType()
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i64 = ir.IntType(64)

# { %"struct.std::atomic", void (i8*, i64, i8*)*, i8*, i8*, i64, i8* }
# %"struct.std::atomic" = type { %"struct.std::__atomic_base" }
# %"struct.std::__atomic_base" = type { i64 }

atomic_i64 = ir.LiteralStructType([i64])
meminfo_t = ir.LiteralStructType([atomic_i64, i8p, i8p, i8p, i64, i8p])
meminfo_ptr_t = meminfo_t.as_pointer()


NRT_MemInfo_init = 'NRT_MemInfo_init'
NRT_MemInfo_alloc = 'NRT_MemInfo_alloc'
NRT_MemInfo_alloc_dtor = 'NRT_MemInfo_alloc_dtor'
NRT_Allocate_External = 'NRT_Allocate_External'
NRT_MemInfo_call_dtor = 'NRT_MemInfo_call_dtor'
nrt_alloc_meminfo_and_data = 'nrt_alloc_meminfo_and_data'
allocate_varlen_buffer = 'allocate_varlen_buffer'

NULL = cgutils.get_null_value(cgutils.voidptr_t)

function_types = {
    NRT_MemInfo_alloc: ir.FunctionType(i8p, [i64]),
    NRT_MemInfo_alloc_dtor: ir.FunctionType(meminfo_ptr_t, [i64, i8p]),
    NRT_MemInfo_init: ir.FunctionType(void, [meminfo_ptr_t, i8p, i64, i8p, i8p, i8p]),
    NRT_Allocate_External: ir.FunctionType(i8p, [i64, i8p]),
    NRT_MemInfo_call_dtor: ir.FunctionType(void, [meminfo_ptr_t]),
    nrt_alloc_meminfo_and_data: ir.FunctionType(i8p, [i64, i8pp, i8p]),
    allocate_varlen_buffer: ir.FunctionType(i8p, [i64, i64]),
}

class RBC_NRT:

    def __init__(self):
        self.module = ir.Module(name='RBC_nrt')

    def _get_function_builder(self, fn_name):
        fn = self._get_function(fn_name)
        block = fn.append_basic_block(name='entry')
        return ir.IRBuilder(block)

    def _get_function(self, fn_name):
        fnty = function_types[fn_name]
        fn = cgutils.get_or_insert_function(self.module, fnty, fn_name)
        return fn

    def _declare_function(self, fn_name, argnames=None):
        fn = self._get_function(fn_name)
        if argnames is not None:
            assert len(argnames) == len(fn.args)
            for arg, name in zip(fn.args, argnames):
                arg.name = name
        return fn

    def define(self):
        for name in dir(self):
            if name.startswith('define_'):
                meth = getattr(self, name)
                meth()

    def define_nrt_meminfo_call_dtor(self):
        self._declare_function(NRT_MemInfo_call_dtor,
                               argnames=('mi',))
        builder = self._get_function_builder(NRT_MemInfo_call_dtor)
        # empty as RBC dont' do memory management in the same way as Numba does
        builder.ret_void()

    def define_nrt_allocate_meminfo_and_data(self):
        # void *nrt_allocate_meminfo_and_data(size_t size,
        #                                     NRT_MemInfo **mi_out,
        #                                     NRT_ExternalAllocator *allocator);
        self._declare_function(nrt_alloc_meminfo_and_data,
                               argnames=('size', 'mi_out', 'allocator'))
        builder = self._get_function_builder(nrt_alloc_meminfo_and_data)
        [size, mi_out, allocator_] = builder.function.args
        # TODO: replace this by `get_abi_sizeof(...)`
        sizeof_meminfo = i64(48)
        alloc_size = builder.add(size, sizeof_meminfo)
        base = builder.call(self._get_function(NRT_Allocate_External),
                            [alloc_size, NULL])
        builder.store(base, mi_out)
        out = builder.gep(base, [sizeof_meminfo], inbounds=True)
        builder.ret(out)

    def define_NRT_Allocate_External(self):
        fn = self._declare_function(NRT_Allocate_External,
                                    argnames=('size', 'allocator'))
        builder = self._get_function_builder(NRT_Allocate_External)

        # allocator is always null as we don't use this argument in RBC/HeavyDB
        [size, _allocator] = fn.args

        allocator = self._get_function(allocate_varlen_buffer)
        elem_size = i64(1)
        ptr = builder.call(allocator, [size, elem_size])
        builder.ret(ptr)

    def define_NRT_MemInfo_alloc_dtor(self):
        self._declare_function(NRT_MemInfo_alloc_dtor,
                               argnames=('size', 'dtor'))
        builder = self._get_function_builder(NRT_MemInfo_alloc_dtor)

        mi = builder.alloca(meminfo_ptr_t, name='mi')

        meminfo_cast = builder.bitcast(mi, i8pp, 'mi_cast')
        size = builder.function.args[0]
        data = builder.call(self._get_function(nrt_alloc_meminfo_and_data),
                            [size, meminfo_cast, NULL], name='data')

        # TODO: check if data is null
        builder.call(self._get_function(NRT_MemInfo_init),
                     [builder.load(mi), data, size, NULL, NULL, NULL])
        builder.ret(builder.load(mi))

    def define_NRT_MemInfo_alloc(self):
        fn = self._declare_function(NRT_Allocate_External,
                                    argnames=('size', 'allocator'))
        builder = self._get_function_builder(NRT_MemInfo_alloc)

        size = fn.args[0]
        alloc_dtor = self._get_function(NRT_MemInfo_alloc_dtor)
        return builder.call(alloc_dtor, [size, NULL])


def create_nrt_functions():
    nrt = RBC_NRT()
    nrt.define()
    return nrt.module
