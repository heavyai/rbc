from numba.core import cgutils
from llvmlite import ir
from contextlib import contextmanager

void = ir.VoidType()
i8 = ir.IntType(8)
i8p = i8.as_pointer()
i8pp = i8p.as_pointer()
i32 = ir.IntType(32)
i64 = ir.IntType(64)

# { %"struct.std::atomic", void (i8*, i64, i8*)*, i8*, i8*, i64, i8* }
# %"struct.std::atomic" = type { %"struct.std::__atomic_base" }
# %"struct.std::__atomic_base" = type { i64 }

REFCT_IDX = 0
DTOR_IDX = 1
DTOR_INFO_IDX = 2
DATA_IDX = 3
SIZE_IDX = 4
EXTERNAL_ALLOCATOR_IDX = 5

# struct MemInfo {
#     std::atomic_size_t     refct;
#     NRT_dtor_function dtor;
#     void              *dtor_info;
#     void              *data;
#     size_t            size;    /* only used for NRT allocated memory */
#     void* external_allocator;
# };

# atomic_i64_t is the llvm representation of std::atomic<int64>, according to godbolt.com
atomic_i64_t = ir.global_context.get_identified_type('atomic_i64')
atomic_i64_t.set_body(i64)

# MemInfo struct
MemInfo_t = ir.global_context.get_identified_type('Struct.MemInfo')
# First argument ought to be "atomic_i64_t" instead of i64
MemInfo_t.set_body(i64, i8p, i8p, i8p, i64, i8p)
MemInfo_ptr_t = MemInfo_t.as_pointer()

sizeof_MemInfo_t = i64(48)


# Function Names
NRT_MemInfo_init = 'NRT_MemInfo_init'

NRT_MemInfo_alloc = 'NRT_MemInfo_alloc'
NRT_MemInfo_alloc_safe = 'NRT_MemInfo_alloc_safe'

NRT_MemInfo_alloc_dtor = 'NRT_MemInfo_alloc_dtor'
NRT_MemInfo_alloc_dtor_safe = 'NRT_MemInfo_alloc_dtor_safe'

NRT_Allocate = 'NRT_Allocate'
NRT_Allocate_External = 'NRT_Allocate_External'
NRT_MemInfo_call_dtor = 'NRT_MemInfo_call_dtor'

NRT_incref = 'NRT_incref'
NRT_decref = 'NRT_decref'

NRT_MemInfo_data_fast = 'NRT_MemInfo_data_fast'

NRT_MemInfo_new = 'NRT_MemInfo_new'
NRT_MemInfo_new_varsize_dtor = 'NRT_MemInfo_new_varsize_dtor'
NRT_MemInfo_varsize_alloc = 'NRT_MemInfo_varsize_alloc'
NRT_MemInfo_varsize_realloc = 'NRT_MemInfo_varsize_realloc'
NRT_MemInfo_varsize_free = 'NRT_MemInfo_varsize_free'
NRT_MemInfo_new_varsize = 'NRT_MemInfo_new_varsize'
NRT_Reallocate = 'NRT_Reallocate'
NRT_dealloc = 'NRT_dealloc'
NRT_Free = 'NRT_Free'
NRT_MemInfo_destroy = 'NRT_MemInfo_destroy'

nrt_internal_custom_dtor = 'nrt_internal_custom_dtor'
nrt_allocate_meminfo_and_data = 'nrt_allocate_meminfo_and_data'
allocate_varlen_buffer = 'allocate_varlen_buffer'
malloc = 'malloc'
free = 'free'
realloc = 'realloc'


function_types = {
    NRT_MemInfo_init: (ir.FunctionType(void, [MemInfo_ptr_t, i8p, i64, i8p, i8p, i8p]),
                       ('mi', 'data', 'size', 'dtor', 'dtor_info', 'external_allocator')),

    NRT_MemInfo_alloc: (ir.FunctionType(MemInfo_ptr_t, [i64]), ('size',)),
    NRT_MemInfo_alloc_safe: (ir.FunctionType(MemInfo_ptr_t, [i64]), ('size',)),

    NRT_MemInfo_alloc_dtor: (ir.FunctionType(MemInfo_ptr_t, [i64, i8p]), ('size', 'dtor')),
    NRT_MemInfo_alloc_dtor_safe: (ir.FunctionType(MemInfo_ptr_t, [i64, i8p]), ('size', 'dtor')),

    NRT_Allocate: (ir.FunctionType(i8p, [i64]), ('size',)),
    NRT_Allocate_External: (ir.FunctionType(i8p, [i64, i8p]), ('size', 'allocator')),
    NRT_MemInfo_call_dtor: (ir.FunctionType(void, [MemInfo_ptr_t]), ('mi',)),

    NRT_incref: (ir.FunctionType(void, [i8p]), ('ptr',)),
    NRT_decref: (ir.FunctionType(void, [i8p]), ('ptr',)),

    NRT_MemInfo_data_fast: (ir.FunctionType(i8p, [i8p]), ('meminfo_ptr',)),
    NRT_MemInfo_new: (ir.FunctionType(MemInfo_ptr_t, [i8p, i64, i8p, i8p]),
                      ('data', 'size', 'dtor', 'dtor_info')),
    NRT_MemInfo_new_varsize: (ir.FunctionType(MemInfo_ptr_t, [i64]), ('size',)),
    NRT_MemInfo_new_varsize_dtor: (ir.FunctionType(MemInfo_ptr_t, [i64, i8p]),
                                   ('size', 'dtor')),
    NRT_MemInfo_varsize_free: (ir.FunctionType(void, [MemInfo_ptr_t, i8p]), ('mi', 'ptr')),
    NRT_MemInfo_varsize_alloc: (ir.FunctionType(i8p, [MemInfo_ptr_t, i64]),
                                ('mi', 'size')),
    NRT_MemInfo_varsize_realloc: (ir.FunctionType(i8p, [MemInfo_ptr_t, i64]),
                                  ('mi', 'size')),
    NRT_Reallocate: (ir.FunctionType(i8p, [i8p, i64]), ('ptr', 'size')),
    NRT_dealloc: (ir.FunctionType(void, [MemInfo_ptr_t]), ('mi',)),
    NRT_Free: (ir.FunctionType(void, [i8p]), ('ptr',)),
    NRT_MemInfo_destroy: (ir.FunctionType(void, [MemInfo_ptr_t]), ('mi',)),

    nrt_internal_custom_dtor: (ir.FunctionType(void, [i8p, i64, i8p]),
                               ('ptr', 'size', 'info')),
    nrt_allocate_meminfo_and_data: (ir.FunctionType(i8p, [i64, i8pp, i8p]),
                                    ('size', 'mi_out', 'allocator')),

    allocate_varlen_buffer: (ir.FunctionType(i8p, [i64, i64]), ()),
    malloc: (ir.FunctionType(i8p, [i64]), ('size',)),
    free: (ir.FunctionType(void, [i8p]), ('ptr',)),
    realloc: (ir.FunctionType(i8p, [i8p, i64]), ()),
}


# defined in nrt.h
NRT_DTOR_FUNCTION = ir.FunctionType(void, [i8p, i64, i8p])


NULL = cgutils.get_null_value(cgutils.voidptr_t)


class RBC_NRT:

    def __init__(self, verbose=False):
        self.module = ir.Module(name='RBC_nrt')
        self.verbose = verbose
        self.define()
        if self.verbose:
            print(self.module)

    def NRT_Debug(self, builder, fmt, *args):
        msg = f"[NRT] {fmt}"
        if not msg.endswith('\n'):
            msg += '\n'
        cgutils.printf(builder, msg, *args)

    def debug_call(self, builder, fn, args, name=()):
        debug_msg = f"calling function {fn.name}"
        with self.debug_ctx(builder, debug_msg):
            ret = builder.call(fn, args, name=name)
        return ret

    @contextmanager
    def debug_ctx(self, builder, debug_msg):
        try:
            cgutils.printf(builder, f'[BEFORE] {debug_msg}\n')
            yield
        finally:
            cgutils.printf(builder, f'[AFTER] {debug_msg}\n')

    def __getattribute__(self, __name):
        if __name in function_types.keys():
            return self._get_function(__name)
        return super().__getattribute__(__name)

    def _get_function_builder(self, fn_name):
        fn = self._get_function(fn_name)
        block = fn.append_basic_block(name='entry')
        return ir.IRBuilder(block)

    def _get_from_meminfo(self, builder, mi, kind):
        table = {'refct': REFCT_IDX,
                 'dtor': DTOR_IDX,
                 'dtor_info': DTOR_INFO_IDX,
                 'data': DATA_IDX,
                 'size': SIZE_IDX,
                 'allocator': EXTERNAL_ALLOCATOR_IDX}
        idx = table[kind]
        return builder.load(builder.gep(mi, [i32(0), i32(idx)]), name=kind)

    def _set_on_meminfo(self, builder, mi, kind, val):
        table = {'refct': REFCT_IDX,
                 'dtor': DTOR_IDX,
                 'dtor_info': DTOR_INFO_IDX,
                 'data': DATA_IDX,
                 'size': SIZE_IDX,
                 'allocator': EXTERNAL_ALLOCATOR_IDX}
        idx = table[kind]
        return builder.store(val, builder.gep(mi, [i32(0), i32(idx)]))

    def _get_function(self, fn_name):
        fnty = function_types[fn_name][0]
        fn = cgutils.get_or_insert_function(self.module, fnty, fn_name)
        return fn

    def _declare_function(self, fn_name, argnames=None):
        fn = self._get_function(fn_name)
        if argnames is not None:
            assert isinstance(argnames, tuple), argnames
            assert len(argnames) == len(fn.args), (len(argnames), len(fn.args))
            for arg, name in zip(fn.args, argnames):
                arg.name = name
        fn.attributes.add("noinline")
        return fn

    def define(self):
        # Ensure for each function in function_types, there is a corresponding
        # implementation
        skip_list = ('allocate_varlen_buffer', 'realloc', 'malloc', 'free')
        for fn_name in function_types.keys():
            if fn_name not in skip_list:
                defn = f'define_{fn_name}'
                getattr(self, defn), defn

        for name in dir(self):
            if name.startswith('define_'):
                meth = getattr(self, name)
                fn_name = name[len('define_'):]
                # TODO: put function names
                self._declare_function(fn_name)
                builder = self._get_function_builder(fn_name)
                args = builder.function.args
                meth(builder, args)

    def define_NRT_MemInfo_call_dtor(self, builder, args):
        [mi] = args
        self.NRT_Debug(builder, "NRT_MemInfo_call_dtor mi=%p\n", mi)

        dtor = builder.load(builder.gep(mi, [i32(0), i32(DTOR_IDX)]))
        not_null = cgutils.is_not_null(builder, dtor)
        with cgutils.if_likely(builder, not_null):
            dtor_fn = builder.bitcast(dtor, NRT_DTOR_FUNCTION.as_pointer())
            data = self._get_from_meminfo(builder, mi, 'data')
            size = self._get_from_meminfo(builder, mi, 'size')
            dtor_info = self._get_from_meminfo(builder, mi, 'dtor_info')
            builder.call(dtor_fn, [data, size, dtor_info])

        builder.call(self.NRT_MemInfo_destroy, [mi])
        builder.ret_void()

    def define_NRT_incref(self, builder, args):
        builder.ret_void()

    def define_NRT_decref(self, builder, args):
        builder.ret_void()

    def define_NRT_MemInfo_data_fast(self, builder, args):
        [ptr] = builder.function.args
        mi_ptr = builder.bitcast(ptr, MemInfo_ptr_t, name='meminfo_ptr')
        data = builder.load(builder.gep(mi_ptr, [i32(0), i32(3)]), name='data')
        builder.ret(data)

    def define_NRT_Reallocate(self, builder, args):
        reallocator = self._get_function(realloc)
        [ptr, size] = args
        new_ptr = builder.call(reallocator, [ptr, size])
        builder.ret(new_ptr)

    def define_NRT_dealloc(self, builder, args):
        [mi] = args
        self.NRT_Debug(builder, "NRT_dealloc meminfo: %p\n", mi)
        ptr = builder.bitcast(mi, i8p)
        builder.call(self.NRT_Free, [ptr])
        builder.ret_void()

    def define_NRT_Free(self, builder, args):
        [ptr] = args
        self.NRT_Debug(builder, "NRT_Free %p\n", ptr)
        builder.call(self.free, [ptr])
        builder.ret_void()

    def define_NRT_MemInfo_varsize_alloc(self, builder, args):
        [mi, size] = args

        data = builder.call(self.NRT_Allocate, [size])
        # TODO: check if data is NULL
        builder.store(data, builder.gep(mi, [i32(0), i32(DATA_IDX)], inbounds=True))
        builder.store(size, builder.gep(mi, [i32(0), i32(SIZE_IDX)], inbounds=True))
        # self._set_on_meminfo(builder, mi, 'data', data)
        # self._set_on_meminfo(builder, mi, 'size', size)
        self.NRT_Debug(builder, "NRT_MemInfo_varsize_alloc %p size=%zu -> data=%p\n",
                       mi, size, data)
        builder.ret(data)
        # return mi->data;

    def define_NRT_MemInfo_varsize_realloc(self, builder, args):
        [mi, size] = args
        data = self._get_from_meminfo(builder, mi, 'data')
        new_data = builder.call(self.NRT_Reallocate, [data, size], name='new_data')

        # TODO: check if new_data is NULL
        self._set_on_meminfo(builder, mi, 'data', data)
        self._set_on_meminfo(builder, mi, 'size', size)
        builder.ret(new_data)

    def define_NRT_MemInfo_varsize_free(self, builder, args):
        [mi, ptr] = args
        builder.call(self.NRT_Free, [ptr])

        mi_data = self._get_from_meminfo(builder, mi, 'data')
        eq = builder.icmp_signed('==', ptr, mi_data)
        with builder.if_then(eq):
            builder.store(NULL, builder.gep(mi, [i32(0), i32(DATA_IDX)]))
        builder.ret_void()

    def define_NRT_MemInfo_destroy(self, builder, args):
        [mi] = args
        self.NRT_Debug(builder, "NRT_MemInfo_destroy mi=%p\n", mi)
        builder.call(self.NRT_dealloc, [mi])
        builder.ret_void()

    def define_nrt_internal_custom_dtor(self, builder, args):
        [ptr, size, info] = args

        dtor = builder.bitcast(info, NRT_DTOR_FUNCTION.as_pointer())
        self.NRT_Debug(builder, "nrt_internal_custom_dtor ptr=%p, info=%p\n",
                       ptr, info)

        not_null = cgutils.is_not_null(builder, dtor)
        with cgutils.if_likely(builder, not_null):
            info_ = NULL
            builder.call(dtor, [ptr, size, info_])
        builder.ret_void()

    def define_nrt_allocate_meminfo_and_data(self, builder, args):
        [size, mi_out, allocator] = args
        # TODO: replace this by `get_abi_sizeof(...)`
        alloc_size = builder.add(size, sizeof_MemInfo_t)
        base = builder.call(self.NRT_Allocate_External,
                            [alloc_size, allocator])
        builder.store(base, mi_out)
        out = builder.gep(base, [sizeof_MemInfo_t], inbounds=True)
        builder.ret(out)

    def define_NRT_Allocate(self, builder, args):
        # allocator is always null as we don't use this argument in RBC/HeavyDB
        [size] = args
        self.NRT_Debug(builder, "NRT_Allocate")
        # allocator = builder.bitcast(self._get_function(malloc), i8p)
        allocator = NULL
        ret = builder.call(self.NRT_Allocate_External, [size, allocator])
        builder.ret(ret)

    def define_NRT_Allocate_External(self, builder, args):
        # allocator is always null as we don't use this argument in RBC/HeavyDB
        [size, allocator_] = args

        # fnty = function_types[malloc][0]
        # allocator = builder.bitcast(allocator_, fnty.as_pointer(), name='external_allocator')
        allocator = self._get_function(malloc)
        ptr = builder.call(allocator, [size])
        self.NRT_Debug(builder,
                       "NRT_Allocate_External allocator=%p bytes=%zu ptr=%p",
                       allocator, size, ptr)
        builder.ret(ptr)

    def define_NRT_MemInfo_alloc_dtor(self, builder, args):
        mi = builder.alloca(MemInfo_ptr_t, name='mi')
        mi_cast = builder.bitcast(mi, i8pp, 'mi_cast')
        [size, dtor] = args
        allocator = NULL
        data = builder.call(self.nrt_allocate_meminfo_and_data,
                            [size, mi_cast, allocator], name='data')

        self.NRT_Debug(builder, "NRT_MemInfo_alloc_dtor %p %zu\n", data, size)
        # TODO: check if data is null
        dtor_function = builder.bitcast(self._get_function(nrt_internal_custom_dtor), i8p)
        dtor_info = builder.bitcast(dtor, i8p)
        allocator = NULL
        builder.call(self.NRT_MemInfo_init,
                     [builder.load(mi), data, size, dtor_function, dtor_info, allocator])
        builder.ret(builder.load(mi))

    def define_NRT_MemInfo_alloc(self, builder, args):
        mi = builder.alloca(MemInfo_ptr_t, name='mi')
        mi_cast = builder.bitcast(mi, i8pp, name='mi_cast')
        [size] = args
        allocator = NULL
        data = builder.call(self.nrt_allocate_meminfo_and_data,
                            [size, mi_cast, allocator], name='data')
        # TODO: check if data is null

        self.NRT_Debug(builder, "NRT_MemInfo_alloc %p\n", data)
        dtor = NULL
        dtor_info = NULL
        allocator = NULL
        builder.call(self.NRT_MemInfo_init,
                     [builder.load(mi), data, size, dtor, dtor_info, allocator])
        builder.ret(builder.load(mi))

    def define_NRT_MemInfo_new_varsize(self, builder, args):
        [size] = args

        # TODO: check if data is null
        data = builder.call(self.NRT_Allocate, [size])
        mi = builder.call(self.NRT_MemInfo_new, [data, size, NULL, NULL], name='mi')
        self.NRT_Debug(builder,
                       "NRT_MemInfo_new_varsize size=%zu -> meminfo=%p, data=%p\n",
                       size, mi, data)
        builder.ret(mi)

    def define_NRT_MemInfo_new(self, builder, args):
        [data, size, dtor, dtor_info] = args
        ptr = builder.call(self.NRT_Allocate, [sizeof_MemInfo_t])
        mi = builder.bitcast(ptr, MemInfo_ptr_t, name='mi')

        is_not_null = cgutils.is_not_null(builder, mi)
        with cgutils.if_likely(builder, is_not_null):
            self.NRT_Debug(builder, "NRT_MemInfo_new mi=%p\n", mi)
            builder.call(self.NRT_MemInfo_init, [mi, data, size, dtor, dtor_info, NULL])
        builder.ret(mi)

    def define_NRT_MemInfo_new_varsize_dtor(self, builder, args):
        [size, dtor] = args
        mi = builder.call(self.NRT_MemInfo_new_varsize, [size])

        is_not_null = cgutils.is_not_null(builder, mi)
        with cgutils.if_likely(builder, is_not_null):
            builder.store(dtor, builder.gep(mi, [i32(0), i32(DTOR_IDX)]))
        builder.ret(mi)

    def define_NRT_MemInfo_alloc_safe(self, builder, args):
        size = builder.function.args[0]
        self.NRT_Debug(builder, "NRT_MemInfo_alloc_safe -> NRT_MemInfo_alloc_dtor_safe\n")
        NRT_dtor_function = NULL
        ret = builder.call(self.NRT_MemInfo_alloc_dtor_safe, [size, NRT_dtor_function])
        builder.ret(ret)

    def define_NRT_MemInfo_alloc_dtor_safe(self, builder, args):
        # wire implementation to unsafe version
        [size, dtor] = args
        data = builder.call(self.NRT_MemInfo_alloc_dtor, [size, dtor], name='data')
        self.NRT_Debug(builder, "NRT_MemInfo_alloc_dtor_safe data=%p size=%zu\n", data, size)
        builder.ret(data)

    def define_NRT_MemInfo_init(self, builder, args):
        [mi, data, size, dtor, dtor_info, external_allocator] = args
        zero = i32(0)

        builder.store(i64(1), builder.gep(mi, [zero, zero], inbounds=True))
        builder.store(dtor, builder.gep(mi, [zero, i32(1)], inbounds=True))
        builder.store(dtor_info, builder.gep(mi, [zero, i32(2)], inbounds=True))
        builder.store(data, builder.gep(mi, [zero, i32(3)], inbounds=True))
        builder.store(size, builder.gep(mi, [zero, i32(4)], inbounds=True))
        builder.store(external_allocator, builder.gep(mi, [zero, i32(5)], inbounds=True))
        self.NRT_Debug(builder, "NRT_MemInfo_init mi=%p\n", mi)
        builder.ret_void()


def create_nrt_functions(debug):
    nrt = RBC_NRT(verbose=debug)
    return nrt.module
