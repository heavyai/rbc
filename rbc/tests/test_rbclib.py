from rbc import rbclib
from .test_numpy_rjit import rjit  # noqa: F401


def test_add_ints_cffi():
    res = rbclib.lib._rbclib_add_ints(20, 22)
    assert res == 42


def test_add_ints_rjit(rjit):  # noqa: F811
    @rjit('int64(int64, int64)')
    def my_add(a, b):
        return rbclib.add_ints(a, b)
    assert my_add(20, 22) == 42
