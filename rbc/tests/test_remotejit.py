import atexit
import pytest
import sys
from rbc.remotejit import RemoteJIT, Signature, Caller
from rbc.typesystem import Type

win32 = sys.platform == 'win32'


def Type_fromstring(s):
    return Type.fromstring(s, None)


@pytest.fixture(scope="module")
def rjit(request):
    rjit = RemoteJIT()
    rjit.start_server(background=True)
    request.addfinalizer(rjit.stop_server)
    atexit.register(rjit.stop_server)
    return rjit


def test_construction():

    rjit = RemoteJIT(local=True)
    device = tuple(rjit.targets)[0]
    target_info = rjit.targets[device]

    assert isinstance(rjit, RemoteJIT)

    # Case 1

    @rjit
    def add(a: int, b: int) -> int:
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 1
    assert signatures[0] == Type_fromstring('i64(i64,i64)')

    # Case 2

    @rjit('double(double, double)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 1
    assert signatures[0] == Type_fromstring('f64(f64,f64)')

    # Case 3

    @rjit('double(double, double)')
    @rjit('int(int, int)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 2
    assert signatures[1] == Type_fromstring('i32(i32,i32)')
    assert signatures[0] == Type_fromstring('f64(f64,f64)')

    # Case 4

    @rjit('double(double, double)',
          'int(int, int)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 2
    assert signatures[0] == Type_fromstring('f64(f64,f64)')
    assert signatures[1] == Type_fromstring('i32(i32,i32)')

    # Case 5

    rjit_int = rjit('int(int, int)')
    rjit_double = rjit('double(double, double)')
    assert isinstance(rjit_int, Signature)

    rjit_types = rjit_int(rjit_double)
    assert isinstance(rjit_types, Signature)

    @rjit_types
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 2
    assert signatures[0] == Type_fromstring('i32(i32,i32)')
    assert signatures[1] == Type_fromstring('f64(f64,f64)')

    # Case 6

    @rjit
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 0

    add.signature('i32(i32,i32)')
    signatures = add.get_signatures(target_info)
    assert len(signatures) == 1
    assert signatures[0] == Type_fromstring('i32(i32,i32)')

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
