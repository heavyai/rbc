import os
from numba.core import cgutils
from numba.cpython.hashing import _hashsecret as hashsecret
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

nrt_debug_incr = 'nrt_debug_incr'
nrt_debug_decr = 'nrt_debug_decr'
nrt_debug_spaces = 'nrt_debug_spaces'
nrt_global_var = '__nrt_global_var'

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

    # debug functions
    nrt_debug_incr: (ir.FunctionType(void, []), ()),
    nrt_debug_decr: (ir.FunctionType(void, []), ()),
    nrt_debug_spaces: (ir.FunctionType(void, []), ()),
}


_debug_functions = (nrt_debug_incr, nrt_debug_decr, nrt_debug_spaces)


# defined in nrt.h
NRT_DTOR_FUNCTION = ir.FunctionType(void, [i8p, i64, i8p])


NULL = cgutils.get_null_value(cgutils.voidptr_t)


class RBC_NRT:

    def __init__(self, verbose=False):
        self.module = ir.Module(name='RBC_nrt')
        self.verbose = verbose
        self.debug_nrt = int(os.environ.get('RBC_DEBUG', False))

        gv = ir.GlobalVariable(self.module, i64, nrt_global_var)
        gv.initializer = i64(0)

        self.define()
        self.set_hashsecrets()

        if self.verbose:
            print(self.module)

    def set_hashsecrets(self):
        for v in hashsecret.values():
            gv = ir.GlobalVariable(self.module, i64, v.symbol)
            gv.initializer = i64(v.value.value)

    def NRT_Debug(self, fmt, *args):
        # only debug if verbose is on
        if self.debug_nrt:
            builder = self.CURRENT_BUILDER

            msg = f"[NRT] {fmt}"
            if not msg.endswith('\n'):
                msg += '\n'

            gv = builder.load(self.module.get_global(nrt_global_var))
            cgutils.printf(builder, "%*d", gv, gv)

            cgutils.printf(builder, msg, *args)

    @contextmanager
    def nrt_debug_ctx(self):
        if self.debug_nrt:
            builder = self.CURRENT_BUILDER
            try:
                builder.call(self.nrt_debug_incr, [])
                self.NRT_Debug(self.CURRENT_FUNCTION)
                yield
            finally:
                builder.call(self.nrt_debug_decr, [])
        else:
            try:
                yield
            finally:
                pass

    def define_nrt_debug_incr(self, builder, args):
        gv = self.module.get_global(nrt_global_var)
        builder.store(builder.add(builder.load(gv), i64(4)), gv)
        builder.ret_void()

    def define_nrt_debug_decr(self, builder, args):
        gv = self.module.get_global(nrt_global_var)
        builder.store(builder.sub(builder.load(gv), i64(4)), gv)
        builder.ret_void()

    def define_nrt_debug_spaces(self, builder, args):
        cgutils.printf(builder, "\t")
        builder.ret_void()

    def __getattribute__(self, __name):
        if __name in function_types.keys():
            return self._get_function(__name)
        return super().__getattribute__(__name)

    def _get_function_builder(self, fn_name):
        fn = self._get_function(fn_name)
        block = fn.append_basic_block(name='entry')
        return ir.IRBuilder(block)

    def _get_from_meminfo(self, mi, kind):
        builder = self.CURRENT_BUILDER
        table = {'refct': REFCT_IDX,
                 'dtor': DTOR_IDX,
                 'dtor_info': DTOR_INFO_IDX,
                 'data': DATA_IDX,
                 'size': SIZE_IDX,
                 'allocator': EXTERNAL_ALLOCATOR_IDX}
        idx = table[kind]
        return builder.load(builder.gep(mi, [i32(0), i32(idx)], inbounds=True),
                            name=kind)

    def _set_on_meminfo(self, mi, kind, val):
        builder = self.CURRENT_BUILDER
        table = {'refct': REFCT_IDX,
                 'dtor': DTOR_IDX,
                 'dtor_info': DTOR_INFO_IDX,
                 'data': DATA_IDX,
                 'size': SIZE_IDX,
                 'allocator': EXTERNAL_ALLOCATOR_IDX}
        idx = table[kind]
        return builder.store(val, builder.gep(mi, [i32(0), i32(idx)], inbounds=True))

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
            if not name.startswith('define_'):
                continue

            if name in _debug_functions and not self.debug_nrt:
                continue

            meth = getattr(self, name)
            fn_name = name[len('define_'):]
            # TODO: put function names
            self._declare_function(fn_name)
            builder = self._get_function_builder(fn_name)
            args = builder.function.args
            self.CURRENT_FUNCTION = fn_name
            self.CURRENT_BUILDER = builder
            meth(builder, args)

    def define_NRT_MemInfo_call_dtor(self, builder, args):
        with self.nrt_debug_ctx():
            [mi] = args
            self.NRT_Debug("NRT_MemInfo_call_dtor mi=%p\n", mi)

            dtor = self._get_from_meminfo(mi, 'dtor')
            not_null = cgutils.is_not_null(builder, dtor)
            with cgutils.if_likely(builder, not_null):
                dtor_fn = builder.bitcast(dtor, NRT_DTOR_FUNCTION.as_pointer())
                data = self._get_from_meminfo(mi, 'data')
                size = self._get_from_meminfo(mi, 'size')
                dtor_info = self._get_from_meminfo(mi, 'dtor_info')
                builder.call(dtor_fn, [data, size, dtor_info])

            builder.call(self.NRT_MemInfo_destroy, [mi])
        builder.ret_void()

    def define_NRT_incref(self, builder, args):
        # with self.nrt_debug_ctx():
        #     pass
        builder.ret_void()

    def define_NRT_decref(self, builder, args):
        # with self.nrt_debug_ctx():
        #     pass
        builder.ret_void()

    def define_NRT_MemInfo_data_fast(self, builder, args):
        with self.nrt_debug_ctx():
            [ptr] = args
            self.NRT_Debug("ptr=%p\n", ptr)
            not_null = builder.icmp_signed('!=', ptr, NULL)
            with builder.if_else(not_null, likely=True) as (then, otherwise):
                with then:
                    bb_then = builder.basic_block
                    mi_ptr = builder.bitcast(ptr, MemInfo_ptr_t, name='meminfo_ptr')
                    data = self._get_from_meminfo(mi_ptr, 'data')
                    self.NRT_Debug("data: %p\n", data)
                with otherwise:
                    # there is a specific case where ptr is NULL and doesn't
                    # crash Numba
                    bb_else = builder.basic_block
                    data_null = NULL
            phi = builder.phi(i8p)
            phi.add_incoming(data, bb_then)
            phi.add_incoming(data_null, bb_else)
        builder.ret(phi)

    def define_NRT_Reallocate(self, builder, args):
        with self.nrt_debug_ctx():
            [ptr, size] = args
            new_ptr = builder.call(self.realloc, [ptr, size])
            self.NRT_Debug("ptr=%p size=%zu -> new_ptr=%p", ptr, size, new_ptr)
        builder.ret(new_ptr)

    def define_NRT_dealloc(self, builder, args):
        with self.nrt_debug_ctx():
            [mi] = args
            self.NRT_Debug("meminfo: %p\n", mi)
            ptr = builder.bitcast(mi, i8p)
            builder.call(self.NRT_Free, [ptr])
        builder.ret_void()

    def define_NRT_Free(self, builder, args):
        with self.nrt_debug_ctx():
            [ptr] = args
            self.NRT_Debug("NRT_Free %p\n", ptr)
            builder.call(self.free, [ptr])
        builder.ret_void()

    def define_NRT_MemInfo_varsize_alloc(self, builder, args):
        with self.nrt_debug_ctx():
            [mi, size] = args

            data = builder.call(self.NRT_Allocate, [size])
            # TODO: check if data is NULL
            self._set_on_meminfo(mi, 'data', data)
            self._set_on_meminfo(mi, 'size', size)
            self.NRT_Debug("%p size=%zu -> data=%p\n",
                           mi, size, data)
        builder.ret(data)

    def define_NRT_MemInfo_varsize_realloc(self, builder, args):
        with self.nrt_debug_ctx():
            [mi, size] = args
            data = self._get_from_meminfo(mi, 'data')
            new_data = builder.call(self.NRT_Reallocate, [data, size], name='new_data')

            # TODO: check if new_data is NULL
            self._set_on_meminfo(mi, 'data', new_data)
            self._set_on_meminfo(mi, 'size', size)
        builder.ret(new_data)

    def define_NRT_MemInfo_varsize_free(self, builder, args):
        with self.nrt_debug_ctx():
            [mi, ptr] = args
            builder.call(self.NRT_Free, [ptr])

            mi_data = self._get_from_meminfo(mi, 'data')
            eq = builder.icmp_signed('==', ptr, mi_data)
            with builder.if_then(eq):
                self._set_on_meminfo(mi, 'data', NULL)
        builder.ret_void()

    def define_NRT_MemInfo_destroy(self, builder, args):
        with self.nrt_debug_ctx():
            [mi] = args
            self.NRT_Debug("mi=%p\n", mi)
            builder.call(self.NRT_dealloc, [mi])
        builder.ret_void()

    def define_nrt_internal_custom_dtor(self, builder, args):
        with self.nrt_debug_ctx():
            [ptr, size, info] = args

            dtor = builder.bitcast(info, NRT_DTOR_FUNCTION.as_pointer())
            self.NRT_Debug("ptr=%p, info=%p dtor=%p\n", ptr, info, dtor)

            not_null = cgutils.is_not_null(builder, dtor)
            with cgutils.if_likely(builder, not_null):
                info_ = NULL
                builder.call(dtor, [ptr, size, info_])
        builder.ret_void()

    def define_nrt_allocate_meminfo_and_data(self, builder, args):
        with self.nrt_debug_ctx():
            [size, mi_out, allocator] = args
            # TODO: replace this by `get_abi_sizeof(...)`
            alloc_size = builder.add(size, sizeof_MemInfo_t)
            base = builder.call(self.NRT_Allocate_External,
                                [alloc_size, allocator])
            builder.store(base, mi_out)
            out = builder.gep(base, [sizeof_MemInfo_t], inbounds=True)
            self.NRT_Debug("base=%p out=%p\n", base, out)
        builder.ret(out)

    def define_NRT_Allocate(self, builder, args):
        # allocator is always null as we don't use this argument in RBC/HeavyDB
        with self.nrt_debug_ctx():
            [size] = args
            allocator = NULL
            ret = builder.call(self.NRT_Allocate_External, [size, allocator])
        builder.ret(ret)

    def define_NRT_Allocate_External(self, builder, args):
        # allocator is always null as we don't use this argument in RBC/HeavyDB
        with self.nrt_debug_ctx():
            [size, allocator_] = args

            ptr = builder.call(self.malloc, [size])
            # allocate_varlen_buffer calls malloc using the formula:
            #   malloc((elem_count + 1) * elem_size)
            # elem_count = builder.sub(size, i64(1), name='element_count')
            # elem_size = i64(1)
            # ptr = builder.call(self.allocate_varlen_buffer, [elem_count, elem_size])
            self.NRT_Debug("bytes=%zu -> ptr=%p", size, ptr)
        builder.ret(ptr)

    def define_NRT_MemInfo_alloc_dtor(self, builder, args):
        with self.nrt_debug_ctx():
            mi = builder.alloca(MemInfo_ptr_t, name='mi')
            mi_cast = builder.bitcast(mi, i8pp, 'mi_cast')
            [size, dtor] = args
            allocator = NULL
            data = builder.call(self.nrt_allocate_meminfo_and_data,
                                [size, mi_cast, allocator], name='data')

            self.NRT_Debug("data=%p size=%zu\n", data, size)
            # TODO: check if data is null
            dtor_function = builder.bitcast(self._get_function(nrt_internal_custom_dtor), i8p)
            dtor_info = builder.bitcast(dtor, i8p)
            allocator = NULL
            builder.call(self.NRT_MemInfo_init,
                         [builder.load(mi), data, size, dtor_function,
                          dtor_info, allocator])
        builder.ret(builder.load(mi))

    def define_NRT_MemInfo_alloc(self, builder, args):
        with self.nrt_debug_ctx():
            mi = builder.alloca(MemInfo_ptr_t, name='mi')
            mi_cast = builder.bitcast(mi, i8pp, name='mi_cast')
            [size] = args
            allocator = NULL
            data = builder.call(self.nrt_allocate_meminfo_and_data,
                                [size, mi_cast, allocator], name='data')
            # TODO: check if data is null

            self.NRT_Debug("%p\n", data)
            dtor = NULL
            dtor_info = NULL
            allocator = NULL
            builder.call(self.NRT_MemInfo_init,
                         [builder.load(mi), data, size, dtor, dtor_info, allocator])
        builder.ret(builder.load(mi))

    def define_NRT_MemInfo_new_varsize(self, builder, args):
        with self.nrt_debug_ctx():
            [size] = args
            self.NRT_Debug("size=%zu\n", size)

            # TODO: check if data is null
            data = builder.call(self.NRT_Allocate, [size])
            mi = builder.call(self.NRT_MemInfo_new, [data, size, NULL, NULL], name='mi')
            self.NRT_Debug("size=%zu -> meminfo=%p, data=%p\n",
                           size, mi, data)
        builder.ret(mi)

    def define_NRT_MemInfo_new(self, builder, args):
        with self.nrt_debug_ctx():
            [data, size, dtor, dtor_info] = args
            ptr = builder.call(self.NRT_Allocate, [sizeof_MemInfo_t])
            mi = builder.bitcast(ptr, MemInfo_ptr_t, name='mi')
            self.NRT_Debug("ptr=%p\n", ptr)

            is_not_null = cgutils.is_not_null(builder, mi)
            with cgutils.if_likely(builder, is_not_null):
                self.NRT_Debug("mi=%p data=%p size=%zu dtor=%p dtor_info=%p\n",
                               mi, data, size, dtor, dtor_info)
                builder.call(self.NRT_MemInfo_init, [mi, data, size, dtor, dtor_info, NULL])
        builder.ret(mi)

    def define_NRT_MemInfo_new_varsize_dtor(self, builder, args):
        with self.nrt_debug_ctx():
            [size, dtor] = args
            self.NRT_Debug("size: %d\n", size)

            mi = builder.call(self.NRT_MemInfo_new_varsize, [size])
            self.NRT_Debug("mi=%p\n", mi)

            is_not_null = cgutils.is_not_null(builder, mi)
            with cgutils.if_likely(builder, is_not_null):
                self._set_on_meminfo(mi, 'dtor', dtor)
        builder.ret(mi)

    def define_NRT_MemInfo_alloc_safe(self, builder, args):
        with self.nrt_debug_ctx():
            [size] = args
            self.NRT_Debug("NRT_MemInfo_alloc_safe -> NRT_MemInfo_alloc_dtor_safe\n")
            NRT_dtor_function = NULL
            ret = builder.call(self.NRT_MemInfo_alloc_dtor_safe, [size, NRT_dtor_function])
        builder.ret(ret)

    def define_NRT_MemInfo_alloc_dtor_safe(self, builder, args):
        with self.nrt_debug_ctx():
            # wire implementation to unsafe version
            [size, dtor] = args
            data = builder.call(self.NRT_MemInfo_alloc_dtor, [size, dtor], name='data')
            self.NRT_Debug("data=%p size=%zu\n", data, size)
        builder.ret(data)

    def define_NRT_MemInfo_init(self, builder, args):
        with self.nrt_debug_ctx():
            [mi, data, size, dtor, dtor_info, external_allocator] = args
            self._set_on_meminfo(mi, 'refct', i64(1))
            self._set_on_meminfo(mi, 'dtor', dtor)
            self._set_on_meminfo(mi, 'dtor_info', dtor_info)
            self._set_on_meminfo(mi, 'data', data)
            self._set_on_meminfo(mi, 'size', size)
            self._set_on_meminfo(mi, 'allocator', external_allocator)
            self.NRT_Debug("mi=%p data=%p size=%zu\n", mi, data, size)
        builder.ret_void()


def create_nrt_functions(debug):
    nrt = RBC_NRT(verbose=debug)
    return nrt.module
