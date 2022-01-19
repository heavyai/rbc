import pytest
from rbc.remotejit import RemoteJIT
from rbc import rbclib
from rbc.rbclib.tracing_allocator import (TracingAllocator, LeakDetector,
                                          InvalidFreeError, MemoryLeakError)
from .test_numpy_rjit import rjit  # noqa: F401


@pytest.fixture
def djit():
    """
    Debug JIT: a RemoteJIT() which automatically uses tracing_allocator and
    detects memory leaks
    """
    return RemoteJIT(local=True, debug=True, use_tracing_allocator=True)


def test_add_ints_cffi():
    res = rbclib.lib._rbclib_add_ints(20, 22)
    assert res == 42


def test_add_ints_rjit(rjit):  # noqa: F811
    @rjit('int64(int64, int64)')
    def my_add(a, b):
        return rbclib.add_ints(a, b)
    assert my_add(20, 22) == 42


class TestTracingAllocator:

    def test_record_allocate(self):
        allocator = TracingAllocator()
        allocator.record_allocate(0x123)
        allocator.record_allocate(0x456)
        assert allocator.seq == 2
        assert allocator.alive_memory == {0x123: 1, 0x456: 2}

    def test_record_free(self):
        allocator = TracingAllocator()
        allocator.record_allocate(0x123)
        allocator.record_allocate(0x456)
        allocator.record_free(0x123)
        allocator.record_allocate(0x789)
        assert allocator.seq == 3
        assert allocator.alive_memory == {0x456: 2, 0x789: 3}

    def test_invalid_free(self):
        allocator = TracingAllocator()
        # 1. raise in case we try to free an unknown pointer
        with pytest.raises(InvalidFreeError):
            allocator.record_free(0x123)

        # 2. raise in case of double free
        allocator.record_allocate(0x456)
        allocator.record_free(0x456)
        assert allocator.alive_memory == {}
        with pytest.raises(InvalidFreeError):
            allocator.record_free(0x456)


class TestLeakDetector:

    def test_no_nested_activation(self):
        allocator = TracingAllocator()
        ld = LeakDetector(allocator)
        with ld:
            with pytest.raises(ValueError):
                with ld:
                    pass

    def test_double_enter(self):
        allocator = TracingAllocator()
        ld = LeakDetector(allocator)
        # check that we can activate the same LeakDetector twice, as long as
        # it's not nested
        with ld:
            pass
        with ld:
            pass

    def test_no_leak(self):
        allocator = TracingAllocator()
        ld = LeakDetector(allocator)
        with ld:
            allocator.record_allocate(0x123)
            allocator.record_allocate(0x456)
            allocator.record_free(0x456)
            allocator.record_free(0x123)

    def test_detect_leak(self):
        allocator = TracingAllocator()
        ld = LeakDetector(allocator)
        with pytest.raises(MemoryLeakError) as exc:
            with ld:
                allocator.record_allocate(0x123)
                allocator.record_allocate(0x456)
                allocator.record_allocate(0x789)
                allocator.record_free(0x123)
        assert exc.value.leaks == [(0x456, 2), (0x789, 3)]


class Test_djit:
    """
    These are the the most important rbclib tests: checks that we can actually
    detect memory leaks inside @djit compiled functions.
    """

    def test_djit_target_info(self, djit):
        targets = djit.targets
        ti = targets['cpu']
        assert ti.name == 'host_cpu_tracing_allocator'
        assert ti.use_tracing_allocator
        assert ti.info['fn_allocate_varlen_buffer'] == 'rbclib_tracing_allocate_varlen_buffer'

    def test_djit_leak(self, djit):
        from rbc.stdlib import array_api as xp

        @djit('int32(int32)')
        def leak_some_memory(size):
            # memory leak!
            a = xp.Array(size, xp.float64)  # noqa: F841
            return size

        with pytest.raises(MemoryLeakError):
            leak_some_memory(10)
