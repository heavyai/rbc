"""
CFFI builder for _rbclib.

This is meant to be listed inside cffi_modules=[...] in setup.py.

To rebuild, run setup.py develop or equivalent.
"""

from cffi import FFI
ffibuilder = FFI()

# rbclib defines two kind of functions:
#
#  1. functions written in C: these are implemented in _rbclib.c and exposed
#     to CFFI by calling ffibuilder.cdef()
#
#  2. functions written in Python: these are exposed to CFFI by using
#     declaring them as extern "C+Python" in ffibuilder.cdef().  CFFI
#     generates a C stub which can be called from C and from JIT-generated
#     code, and which in turns call the Python function which is defined using
#     @ffi.def_extern.
#
# Moreover, we need to take extra care to support Windows.  Contrarily to
# Unix-like systems, Windows requires symbols to be explicitly exported in
# order to be visible in the generated DLL/.pyd file. RBC_DLLEXPORT


ffibuilder.cdef("""
int64_t _rbclib_add_ints(int64_t a, int64_t b);
int8_t* rbclib_allocate_varlen_buffer(int64_t element_count, int64_t element_size);
void rbclib_free_buffer(int8_t *addr);

extern "C+Python" {
    int8_t* rbclib_tracing_allocate_varlen_buffer(int64_t element_count,
                                                  int64_t element_size);
    void rbclib_tracing_free_buffer(int8_t *addr);
}
""")

ffibuilder.set_source(
    "rbc.rbclib._rbclib",
    source='#include "_rbclib.h"',
    include_dirs=['rbc/rbclib'],
    sources=['rbc/rbclib/_rbclib.c'],
)
