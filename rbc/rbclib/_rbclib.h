#include <stdint.h>

#if defined(_MSC_VER)
#  define RBC_DLLEXPORT  extern __declspec(dllexport)
#else
#  define RBC_DLLEXPORT  extern
#endif

/* functions implemented in C */
RBC_DLLEXPORT int64_t _rbclib_add_ints(int64_t a, int64_t b);
RBC_DLLEXPORT int8_t* rbclib_allocate_varlen_buffer(int64_t element_count, int64_t element_size);
RBC_DLLEXPORT void rbclib_free_buffer(int8_t *addr);

/* functions implemented in Python and declared as extern "C+Python" in
   ffibuilder.cdef() */
RBC_DLLEXPORT int8_t* rbclib_debug_allocate_varlen_buffer(int64_t element_count,
                                                          int64_t element_size);
RBC_DLLEXPORT void rbclib_debug_free_buffer(int8_t *addr);
