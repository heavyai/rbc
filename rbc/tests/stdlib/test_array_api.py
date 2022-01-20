from rbc.stdlib import array_api as xp
from rbc.omnisci_backend.omnisci_buffer import free_buffer
from ..test_rbclib import djit  # noqa: F401


def test_array_free_function_call(djit):    # noqa: F811

    @djit('int32(int32)')
    def fn(size):
        a = xp.Array(size, xp.float64)
        free_buffer(a)
        return size

    res = fn(10)
    assert res == 10


def test_array_free_method(djit):    # noqa: F811

    @djit('int32(int32)')
    def fn(size):
        a = xp.Array(size, xp.float64)
        a.free()
        return size

    res = fn(10)
    assert res == 10


def test_array_constructor_noreturn(djit):    # noqa: F811

    @djit('float64(int32)')
    def array_noreturn(size):
        a = xp.Array(size, xp.float64)
        b = xp.Array(size, xp.float64)
        c = xp.Array(size, xp.float64)
        for i in range(size):
            a[i] = b[i] = c[i] = i + 3.0
        s = 0.0
        for i in range(size):
            s += a[i] + b[i] + c[i] - a[i] * b[i]
        a.free()
        b.free()
        c.free()
        return s

    res = array_noreturn(10)
    assert res == -420
