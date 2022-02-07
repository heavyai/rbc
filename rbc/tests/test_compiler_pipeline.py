import pytest
import contextlib
import warnings
from rbc.remotejit import RemoteJIT
from rbc.omnisci_backend.omnisci_buffer import free_buffer
from rbc.omnisci_backend.omnisci_pipeline import MissingFreeWarning, MissingFreeError
from rbc.stdlib import array_api as xp


@pytest.fixture
def rjit():
    return RemoteJIT(local=True)


@contextlib.contextmanager
def no_warnings(warning_cls):
    with pytest.warns(None) as wlist:
        yield

    wlist = [w.message for w in wlist if isinstance(w.message, warning_cls)]
    if wlist:
        raise AssertionError(
            "Warnings were raised: " + ", ".join([str(w) for w in wlist])
        )


def test_no_warnings_decorator():
    with no_warnings(MissingFreeWarning):
        pass

    with no_warnings(MissingFreeWarning):
        warnings.warn('hello')

    with pytest.raises(AssertionError, match='Warnings were raised'):
        with no_warnings(MissingFreeWarning):
            warnings.warn(MissingFreeWarning('hello'))


class TestDetectMissingFree:

    def test_on_missing_free_warn(self, rjit):
        # basic case: we are creating an array but we don't call .free()
        @rjit('int32(int32)')
        def my_func(size):
            a = xp.Array(size, xp.float64)  # noqa: F841
            return size

        with pytest.warns(MissingFreeWarning, match='by function my_func'):
            res = my_func(10)
            assert res == 10

    def test_on_missing_free_fail(self, rjit):
        @rjit('int32(int32)', on_missing_free='fail')
        def my_func(size):
            a = xp.Array(size, xp.float64)  # noqa: F841
            return size

        with pytest.raises(MissingFreeError, match='by function my_func'):
            res = my_func(10)

    def test_on_missing_free_ignore(self, rjit):
        @rjit('int32(int32)', on_missing_free='ignore')
        def fn(size):
            a = xp.Array(size, xp.float64)  # noqa: F841
            return size

        with no_warnings(MissingFreeWarning):
            res = fn(10)
            assert res == 10

    def test_set_on_missing_globally(self):
        my_rjit = RemoteJIT(local=True, on_missing_free='fail')
        @my_rjit('int32(int32)')
        def fn(size):
            a = xp.Array(size, xp.float64)  # noqa: F841
            return size

        # this raises because on_missing_free='fail' it set globablly on the
        # RemoteJIT instance
        with pytest.raises(MissingFreeError):
            res = fn(10)

    def test_detect_call_to_free_buffer(self, rjit):
        @rjit('int32(int32)')
        def fn(size):
            a = xp.Array(size, xp.float64)
            free_buffer(a)
            return size

        with no_warnings(MissingFreeWarning):
            res = fn(10)
            assert res == 10

    def test_detect_call_to_free_buffer_non_global(self, rjit):
        # note, this is not a typo: we are aware that free_buffer is already
        # imported globally, but here we want to check that we detect the call
        # to free_buffer even when it's imported locally
        from rbc.omnisci_backend.omnisci_buffer import free_buffer

        @rjit('int32(int32)')
        def fn(size):
            a = xp.Array(size, xp.float64)
            free_buffer(a)
            return size

        with no_warnings(MissingFreeWarning):
            res = fn(10)
            assert res == 10

    def test_detect_call_to_method_free(self, rjit):
        @rjit('int32(int32)')
        def fn(size):
            a = xp.Array(size, xp.float64)
            a.free()
            return size

        with no_warnings(MissingFreeWarning):
            res = fn(10)
            assert res == 10
