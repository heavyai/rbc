from ._rbclib import lib, ffi


class DebugAllocator:

    def __init__(self):
        self.allocs = {}
        self.n = 0

    def record_allocate(self, addr):
        self.allocs[addr] = self.n
        self.n += 1

    def record_free(self, addr):
        if addr not in self.allocs:
            raise Exception('Trying to free() a dangling pointer?')
        del self.allocs[addr]


# global singleton
_ALLOCATOR = DebugAllocator()


@ffi.def_extern()
def rbclib_debug_allocate_varlen_buffer(element_count, element_size):
    addr = lib.rbclib_allocate_varlen_buffer(element_count, element_size)
    _ALLOCATOR.record_allocate()
    return addr


@ffi.def_extern()
def rbclib_debug_free_buffer(addr):
    _ALLOCATOR.record_free(addr)
    lib.rbclib_free_buffer(addr)
