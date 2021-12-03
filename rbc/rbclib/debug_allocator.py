from ._rbclib import lib, ffi

class DebugAllocatorError(Exception):
    pass

class InvalidFreeError(DebugAllocatorError):
    pass


class DebugAllocator:
    """
    Provide debug versions of allocate_varlen_buffer and free_buffer to detect
    memory leak.

    The logic is written in pure Python, and it is exposed to the C world
    through CFFI's @def_extern() mechanism.
    """

    def __init__(self):
        # alive_memory is a dictionary which contains all the addresses which
        # have been allocated but not yet freed.  For each address we record
        # an unique sequence number which acts as a timestamp, so that we can
        # inspect them in allocation order.
        self.seq = 0
        self.alive_memory = {}  # {address: seq}

    def record_allocate(self, addr):
        self.seq += 1
        self.alive_memory[addr] = self.seq

    def record_free(self, addr):
        if addr not in self.alive_memory:
            raise InvalidFreeError('Trying to free() a dangling pointer?')
        del self.alive_memory[addr]


# global singleton
_ALLOCATOR = DebugAllocator()


@ffi.def_extern()
def rbclib_debug_allocate_varlen_buffer(element_count, element_size):
    addr = lib.rbclib_allocate_varlen_buffer(element_count, element_size)
    _ALLOCATOR.record_allocate(addr)
    return addr


@ffi.def_extern()
def rbclib_debug_free_buffer(addr):
    _ALLOCATOR.record_free(addr)
    lib.rbclib_free_buffer(addr)
