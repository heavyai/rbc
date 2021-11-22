from ..test_numpy_rjit import rjit  # noqa: F401
from rbc.stdlib import array_api as xp


def test_array_constructor_noreturn(rjit):    # noqa: F811

    @rjit('float64(int32)')
    def array_noreturn(size):
        a = xp.Array(size, xp.float64)
        b = xp.Array(size, xp.float64)
        c = xp.Array(size, xp.float64)
        for i in range(size):
            a[i] = b[i] = c[i] = i + 3.0
        s = 0.0
        for i in range(size):
            s += a[i] + b[i] + c[i] - a[i] * b[i]
        return s

    res = array_noreturn(10)
    assert res == -420
