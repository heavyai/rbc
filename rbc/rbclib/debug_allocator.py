from ._rbclib import lib, ffi

class DebugAllocatorError(Exception):
    pass


class InvalidFreeError(DebugAllocatorError):
    pass


class MemoryLeakError(DebugAllocatorError):

    def __init__(self, leaks):
        lines = [f'Found {len(leaks)} memory leaks:']
        for addr, seq in leaks:
            lines.append(f'    {addr} (seq = {seq})')
        message = '\n'.join(lines)
        super().__init__(message)
        self.leaks = leaks


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


class LeakDetector:
    """
    Context manager to detect memory leaks on the given allocator.

    When we enter the context manager, we record the current sequence
    number. Upon exit, we check that all the new allocations have been freed.
    """

    def __init__(self, allocator):
        self.allocator = allocator
        self.start_seq = None

    def __enter__(self):
        if self.start_seq is not None:
            raise ValueError('LeakDetector already active')
        self.start_seq = self.allocator.seq

    def __exit__(self, etype, evalue, tb):
        leaks = []
        for addr, seq in self.allocator.alive_memory.items():
            if seq > self.start_seq:
                leaks.append((addr, seq))
        self.start_seq = None
        if leaks:
            leaks.sort(key=lambda t: t[1]) # sort by seq
            raise MemoryLeakError(leaks)


# global singleton
_ALLOCATOR = DebugAllocator()

def new_leak_detector():
    """
    Return a new instance of LeakDetector associated to the global debug
    allocator
    """
    return LeakDetector(_ALLOCATOR)


@ffi.def_extern()
def rbclib_debug_allocate_varlen_buffer(element_count, element_size):
    addr = lib.rbclib_allocate_varlen_buffer(element_count, element_size)
    _ALLOCATOR.record_allocate(addr)
    return addr


@ffi.def_extern()
def rbclib_debug_free_buffer(addr):
    _ALLOCATOR.record_free(addr)
    lib.rbclib_free_buffer(addr)
