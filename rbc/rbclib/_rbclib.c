#include <stdlib.h>
#include "_rbclib.h"


// trivial function, used to test that the basic machinery works
RBC_DLLEXPORT int64_t _rbclib_add_ints(int64_t a, int64_t b) {
    return a + b;
}

// NOTE: allocate_varlen_buffer must have the same signature as the one
// defined by omniscidb
RBC_DLLEXPORT int8_t *rbclib_allocate_varlen_buffer(int64_t element_count, int64_t element_size) {
    size_t size = element_count * element_size;
    // malloc(0) is allowed to return NULL. But here we want to ensure that we
    // return NULL only to signal an out of memory error, so we make sure to
    // always allocate at least 1 byte
    if (size == 0)
        size = 1;
    return (int8_t *)malloc(size);
}

RBC_DLLEXPORT void rbclib_free_buffer(int8_t *addr) {
    free((void*)addr);
}
