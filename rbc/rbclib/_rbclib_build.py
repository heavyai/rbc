"""
CFFI builder for _rbclib.

This is meant to be listed inside cffi_modules=[...] in setup.py.

To rebuild, run setup.py develop or equivalent.
"""

from cffi import FFI
ffibuilder = FFI()

# C prototypes of the functions that we want to expose. Note that this is used
# for both ffibuilder.cdef and ffibuilder.set_source.
function_prototypes = """
int64_t _rbclib_add_ints(int64_t a, int64_t b);
"""

ffibuilder.cdef(function_prototypes)
ffibuilder.set_source(
    "rbc.rbclib._rbclib",
    source=function_prototypes,
    sources=['rbc/rbclib/_rbclib.c'],
)
