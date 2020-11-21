import atexit
import pytest
import sys
from rbc.remotejit import RemoteJIT, Signature, Caller
from rbc.typesystem import Type

win32 = sys.platform == 'win32'


@pytest.fixture(scope="module")
def rjit(request):
    rjit = RemoteJIT(debug=True)
    rjit.start_server(background=True)
    request.addfinalizer(rjit.stop_server)
    atexit.register(rjit.stop_server)
    return rjit


def with_localjit(test_func):
    ljit = RemoteJIT(local=True)
    device = tuple(ljit.targets)[0]
    target_info = ljit.targets[device]

    def test_func_():
        with target_info:
            return test_func(ljit)
    return test_func_


@with_localjit
def test_construction(ljit):

    assert isinstance(ljit, RemoteJIT)

    # Case 1

    @ljit
    def add(a: int, b: int) -> int:
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures()
    assert len(signatures) == 1
    assert signatures[0] == Type.fromstring('i64(i64,i64)')

    # Case 2

    @ljit('double(double, double)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures()
    assert len(signatures) == 1
    assert signatures[0] == Type.fromstring('f64(f64,f64)')

    # Case 3

    @ljit('double(double, double)')
    @ljit('int(int, int)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures()
    assert len(signatures) == 2
    assert signatures[1] == Type.fromstring('i32(i32,i32)')
    assert signatures[0] == Type.fromstring('f64(f64,f64)')

    # Case 4

    @ljit('double(double, double)',
          'int(int, int)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures()
    assert len(signatures) == 2
    assert signatures[0] == Type.fromstring('f64(f64,f64)')
    assert signatures[1] == Type.fromstring('i32(i32,i32)')

    # Case 5

    ljit_int = ljit('int(int, int)')
    ljit_double = ljit('double(double, double)')
    assert isinstance(ljit_int, Signature)

    ljit_types = ljit_int(ljit_double)
    assert isinstance(ljit_types, Signature)

    @ljit_types
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures()
    assert len(signatures) == 2
    assert signatures[0] == Type.fromstring('i32(i32,i32)')
    assert signatures[1] == Type.fromstring('f64(f64,f64)')

    # Case 6

    @ljit
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures()
    assert len(signatures) == 0

    add.signature('i32(i32,i32)')
    signatures = add.get_signatures()
    assert len(signatures) == 1
    assert signatures[0] == Type.fromstring('i32(i32,i32)')

    # invalid cases are now handled at compile stage


def test_return_scalar(rjit):

    @rjit('i64(i64)', 'f64(f64)', 'c128(c128)')
    def ret(a):
        return a

    r = ret(123)
    assert r == 123
    assert isinstance(r, int)

    r = ret(123.45)
    assert r == 123.45
    assert isinstance(r, float)

    if not win32:
        # see https://github.com/xnd-project/rbc/issues/4
        r = ret(123+45j)
        assert r == 123+45j
        assert isinstance(r, complex)


def test_rjit_add(rjit):

    @rjit('i64(i64,i64)')
    def add(a, b):
        return a + b

    assert isinstance(add(1, 2), int)
    assert add(1, 2) == 3

    with pytest.raises(
            TypeError,
            match=r'found no matching function type to given argument types'):
        add(1, 2.5)

    add.signature('d(d,d)')

    assert isinstance(add(1, 2.5), float)
    assert add(1, 2.5) == 3.5

    assert isinstance(add(1.0, 2.5), float)
    assert add(1.0, 2.5) == 3.5

    with pytest.raises(
            TypeError,
            match=r'found no matching function type to given argument types'):
        add(1j, 2)

    add.signature('c128(c128,c128)')

    if not win32:
        # see https://github.com/xnd-project/rbc/issues/4
        r = add(1j, 2j)
        assert isinstance(r, complex)
        assert r == 3j
        assert add(1j, 2) == 2+1j


def test_options_local(rjit):

    @rjit
    def foo(x: int) -> int:
        return x + 1

    assert foo(1) == 2   # remote execution
    assert foo.local(1) == 2  # local execution


def test_composition(rjit):
    import numba as nb

    @nb.njit
    def fun(x):
        return x*x

    @rjit(local=True)
    def bar(x: int) -> int:
        y = fun(x)
        return y

    # print(bar.get_IR())
