import pytest
from rbc import rbclib
from rbc.rbclib.debug_allocator import DebugAllocator, InvalidFreeError
from .test_numpy_rjit import rjit  # noqa: F401


def test_add_ints_cffi():
    res = rbclib.lib._rbclib_add_ints(20, 22)
    assert res == 42


def test_add_ints_rjit(rjit):  # noqa: F811
    @rjit('int64(int64, int64)')
    def my_add(a, b):
        return rbclib.add_ints(a, b)
    assert my_add(20, 22) == 42


class TestDebugAllocator:

    def test_record_allocate(self):
        allocator = DebugAllocator()
        allocator.record_allocate(0x123)
        allocator.record_allocate(0x456)
        assert allocator.seq == 2
        assert allocator.alive_memory == {0x123: 1, 0x456: 2}

    def test_record_free(self):
        allocator = DebugAllocator()
        allocator.record_allocate(0x123)
        allocator.record_allocate(0x456)
        allocator.record_free(0x123)
        allocator.record_allocate(0x789)
        assert allocator.seq == 3
        assert allocator.alive_memory == {0x456: 2, 0x789: 3}

    def test_invalid_free(self):
        allocator = DebugAllocator()
        # 1. raise in case we try to free an unknown pointer
        with pytest.raises(InvalidFreeError):
            allocator.record_free(0x123)

        # 2. raise in case of double free
        allocator.record_allocate(0x456)
        allocator.record_free(0x456)
        assert allocator.alive_memory == {}
        with pytest.raises(InvalidFreeError):
            allocator.record_free(0x456)
