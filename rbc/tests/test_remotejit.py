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


@with_localjit
def test_templates(ljit):

    assert isinstance(ljit, RemoteJIT)

    def check_normalized(sig1, sig2):
        sig1 = set(sig1.normalized().signatures)
        sig2 = set(sig2.normalized().signatures)
        assert sig1 == sig2

    # API

    check_normalized(ljit('T(T)', templates=dict(T=['i8', 'i4'])),
                     ljit('i8(i8)', 'i4(i4)'))

    check_normalized(ljit('T(T*)', templates=dict(T=['i8', 'i4'])),
                     ljit('i8(i8*)', 'i4(i4*)'))

    check_normalized(ljit('T({T})', templates=dict(T=['i8', 'i4'])),
                     ljit('i8({i8})', 'i4({i4})'))

    check_normalized(ljit('T(T2)', templates=dict(T=['i8', 'i4'], T2=['i8', 'i4'])),
                     ljit('i8(i8)', 'i4(i4)', 'i8(i4)', 'i4(i8)'))

    check_normalized(ljit('T(T2*)', templates=dict(T=['i8', 'i4'], T2=['i8', 'i4'])),
                     ljit('i8(i8*)', 'i4(i4*)', 'i8(i4*)', 'i4(i8*)'))

    check_normalized(ljit('T({T2})', templates=dict(T=['i8', 'i4'], T2=['i8', 'i4'])),
                     ljit('i8({i8})', 'i4({i4})', 'i8({i4})', 'i4({i8})'))

    check_normalized(ljit('T({T2, T})', templates=dict(T=['i8', 'i4'], T2=['i8', 'i4'])),
                     ljit('i8({i8, i8})', 'i4({i4, i4})', 'i8({i4, i8})', 'i4({i8, i4})'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8', 'i4'], U=['f32'])),
                     ljit('i8(f32)', 'i4(f32)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8', 'i4'], U=['T'])),
                     ljit('i8(i8)', 'i4(i4)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8', 'i4'], U=['T*'])),
                     ljit('i8(i8*)', 'i4(i4*)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8', 'i4'], U=['T*', '{T*}'])),
                     ljit('i8(i8*)', 'i4(i4*)', 'i8({i8*})', 'i4({i4*})'))

    # recursion

    with pytest.raises(
            TypeError,
            match=r"cannot make T concrete"):
        ljit('T(U)', templates=dict(T=['U'], U=['T'])).normalized()

    with pytest.raises(
            TypeError,
            match=r"cannot make T concrete"):
        ljit('T(U)', templates=dict(T=['U'], U=['V'], V=['T'])).normalized()

    # not a concrete type

    with pytest.raises(
            TypeError,
            match=r"cannot make T concrete"):
        ljit('T(T)').normalized()

    # custom types

    class MyClass(Type):
        pass

    class MyClass2(Type):
        pass

    check_normalized(ljit('T(MyClass<T>)', templates=dict(T=['i8', 'i4'])),
                     ljit('i8(MyClass<int8>)', 'int4(MyClass<i4>)'))

    check_normalized(ljit('T(MyClass3<T>)', templates=dict(T=['i8', 'i4'])),
                     ljit('i8(MyClass3<int8>)', 'int4(MyClass3<i4>)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8', 'i4'], U=['MyClass<T>'])),
                     ljit('i8(MyClass<int8>)', 'int4(MyClass<i4>)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8', 'i4'], U=['MyClass2<T>'])),
                     ljit('i8(MyClass2<int8>)', 'int4(MyClass2<i4>)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8'], U=['C<T>'],
                                                 C=['MyClass2', 'MyClass3'])),
                     ljit('i8(MyClass2<int8>)', 'i8(MyClass3<int8>)'))

    check_normalized(ljit('T(U)', templates=dict(T=['i8'], U=['C<T>'],
                                                 C=['MyClass', 'MyClass2', 'MyClass3'])),
                     ljit('i8(MyClass2<int8>)', 'i8(MyClass<int8>)', 'i8(MyClass3<int8>)'))

    # user-friendly interface

    check_normalized(ljit('T(T)', T=['i8', 'i4']),
                     ljit('i8(i8)', 'i4(i4)'))

    check_normalized(ljit('T(U)', T=['i8', 'i4'], U=['MyClass<T>']),
                     ljit('i8(MyClass<int8>)', 'int4(MyClass<i4>)'))

    check_normalized(ljit('T(U)', T=['i8', 'i4'], U=['MyClass3<T>']),
                     ljit('i8(MyClass3<int8>)', 'int4(MyClass3<i4>)'))
