"""
CFFI builder for _rbclib.

This is meant to be listed inside cffi_modules=[...] in setup.py.

To rebuild, run setup.py develop or equivalent.
"""

from cffi import FFI
ffibuilder = FFI()

# C prototypes of the functions that we want to expose. Note that this is used
# for both ffibuilder.cdef and ffibuilder.set_source.
C_funcs = """
int64_t _rbclib_add_ints(int64_t a, int64_t b);
int8_t* rbclib_allocate_varlen_buffer(int64_t element_count, int64_t element_size);
void rbclib_free_buffer(int8_t *addr);
"""

Py_funcs = """
extern "C+Python" {
    int8_t* rbclib_debug_allocate_varlen_buffer(int64_t element_count,
                                                int64_t element_size);
    void rbclib_debug_free_buffer(int8_t *addr);
}
"""

ffibuilder.cdef(C_funcs + Py_funcs)
ffibuilder.set_source(
    "rbc.rbclib._rbclib",
    source=C_funcs,
    sources=['rbc/rbclib/_rbclib.c'],
)
