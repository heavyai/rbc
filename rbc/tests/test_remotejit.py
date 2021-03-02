import atexit
import pytest
import sys
import ctypes
import numpy as np
from rbc.remotejit import RemoteJIT, Signature, Caller
from rbc.typesystem import Type
from rbc.external import external
from rbc.externals.macros import sizeof, cast

win32 = sys.platform == 'win32'


@pytest.fixture(scope="module")
def rjit(request):
    local = False
    rjit = RemoteJIT(debug=not True, local=local)
    if not local:
        rjit.start_server(background=True)
        request.addfinalizer(rjit.stop_server)
        atexit.register(rjit.stop_server)
    return rjit


@pytest.fixture(scope="module")
def ljit(request):
    ljit = RemoteJIT(debug=not True, local=True)
    return ljit


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


scalar_pointer_types = ['int64', 'int32', 'int16', 'int8', 'float32', 'float64', 'bool', 'void']


@pytest.mark.parametrize("T", scalar_pointer_types)
@pytest.mark.parametrize("V", scalar_pointer_types)
def test_scalar_pointer_conversion(rjit, T, V):
    with Type.alias(T=T, V=V, intp='int64'):

        # pointer-intp conversion

        @rjit('T*(intp a)')
        def i2p(a):
            return a

        @rjit('V*(intp a)')
        def i2r(a):
            return a

        @rjit('intp(T* a)')
        def p2i(a):
            return a

        @rjit('intp(V* a)')
        def r2i(a):
            return a

        @rjit('T*(T* a)')
        def p2p(a):
            return a

        @rjit('T*(V* a)')
        def p2r(a):
            return a

        @rjit('V*(T* a)')
        def r2p(a):
            return a

        # pointer artithmetics

        @rjit('T*(T* a, intp i)')
        def padd(a, i):
            return a + i

        @rjit('T*(T* a)')
        def pincr(a):
            return a + 1

        if T == V:
            i = 79
            p = i2p(i)
            assert not isinstance(p, int), (type(p), p)
            assert ctypes.cast(p, ctypes.c_void_p).value == i
            i2 = p2i(p)
            assert i2 == i
            p2 = p2p(p)
            assert ctypes.cast(p2, ctypes.c_void_p).value == ctypes.cast(p, ctypes.c_void_p).value
            assert p2i(padd(p, 2)) == i + 2
            assert p2i(pincr(p)) == i + 1

        if 'void' in [T, V]:
            i = 85
            r = i2r(i)
            assert not isinstance(r, int), (type(r), r)

            assert ctypes.cast(r, ctypes.c_void_p).value == i
            p2 = r2p(r)
            assert p2i(p2) == i
            assert p2i(p2r(i2p(i))) == i

        # double pointer-intp conversion

        @rjit('T**(intp a)')
        def i2pp(a):
            return a

        @rjit('V**(intp a)')
        def i2rr(a):
            return a

        @rjit('intp(T** a)')
        def pp2i(a):
            return a

        @rjit('intp(V** a)')
        def rr2i(a):
            return a

        @rjit('T**(T** a)')
        def pp2pp(a):
            return a

        @rjit('T**(T** a, intp i)')
        def ppadd(a, i):
            return a + i

        @rjit('T**(T** a)')
        def ppincr(a):
            return a + 1

        if T == V:
            i = 89
            pp = i2pp(i)
            assert ctypes.cast(pp, ctypes.c_void_p).value == i
            i2 = pp2i(pp)
            assert i2 == i
            pp2 = pp2pp(pp)
            assert ctypes.cast(pp2, ctypes.c_void_p).value == ctypes.cast(
                pp, ctypes.c_void_p).value
            assert pp2i(ppadd(pp, 2)) == i + 2
            assert pp2i(ppincr(pp)) == i + 1

        # double pointer-pointer conversion

        @rjit('T**(T* a)')
        def p2pp(a):
            return a

        @rjit('T*(T** a)')
        def pp2p(a):
            return a

        # mixed pointer conversion requires void*

        @rjit('T**(V* a)')
        def r2pp(a):
            return a

        @rjit('V*(T** a)')
        def pp2r(a):
            return a

        if T == V == 'void':
            i = 99
            p = i2p(i)
            pp = p2pp(p)
            assert pp2i(pp) == i
            assert p2i(pp2p(pp)) == i

        if V == 'void':
            i = 105
            assert r2i(pp2r(i2pp(i))) == i
            assert pp2i(r2pp(i2r(i))) == i


@pytest.mark.parametrize("T", ['int64', 'int32', 'int16', 'int8', 'float32', 'float64'])
def test_scalar_pointer_access_local(ljit, T):

    with Type.alias(T=T):
        arr = np.array([1, 2, 3, 4], dtype=T)
        arr2 = np.array([11, 12, 13, 14], dtype=T)

        ptr = ctypes.c_void_p(arr.__array_interface__['data'][0])

        @ljit('T(T* x, int i)')
        def pitem(x, i):
            return x[i]

        @ljit('void(T* x, int i, T)')
        def sitem(x, i, v):
            x[i] = v

        for i in range(len(arr)):
            v = pitem(ptr, i)
            assert v == arr[i]

        for i in range(len(arr)):
            sitem(ptr, i, arr2[i])

        assert (arr == arr2).all()


memory_managers = dict(
    cstdlib=('void* calloc(size_t nmemb, size_t size)',
             'void free(void* ptr)'),
    PyMem_Raw=(
        'void* PyMem_RawCalloc(size_t nmemb, size_t size)',
        'void PyMem_RawFree(void* ptr)'),
    PyMem=(
        'void* PyMem_Calloc(size_t nmemb, size_t size)',
        'void PyMem_Free(void* ptr)'),
)


@pytest.mark.parametrize("T", ['int64', 'int32', 'int16', 'int8', 'float32', 'float64'])
@pytest.mark.parametrize("memman", list(memory_managers))
def test_scalar_pointer_access_remote(rjit, memman, T):
    calloc_prototype, free_prototype = memory_managers[memman]

    with Type.alias(T=T):

        arr = np.array([1, 2, 3, 4], dtype=T)
        arr2 = np.array([11, 12, 13, 14], dtype=T)

        calloc = external(calloc_prototype)
        free = external(free_prototype)

        @rjit(calloc_prototype)
        def rcalloc(nmemb, size):
            return calloc(nmemb, size)

        @rjit(free_prototype)
        def rfree(ptr):
            return free(ptr)

        @rjit('T(T* x, int i)')
        def pitem(x, i):
            return x[i]

        @rjit('void(T* x, int i, T)')
        def sitem(x, i, v):
            x[i] = v

        ptr = rcalloc(arr.itemsize, len(arr))

        assert ptr.value

        for i in range(len(arr)):
            sitem(ptr, i, arr[i])

        for i in range(len(arr2)):
            arr2[i] = pitem(ptr, i)

        rfree(ptr)

        assert (arr == arr2).all()


@pytest.mark.parametrize("location", ['local', 'remote'])
@pytest.mark.parametrize("T", ['int64', 'int32', 'int16', 'int8', 'float32', 'float64'])
def test_struct_input(ljit, rjit, location, T):
    jit = rjit if location == 'remote' else ljit

    S = '{T x, T y, T z}'

    with Type.alias(T=T, S=S):

        nb_S = Type.fromstring(S).tonumba()
        nb_T = Type.fromstring(T).tonumba()

        class MetaMyStructType(type):

            @property
            def __typesystem_type__(cls):
                # Python type dependent type
                return Type.fromstring(S) | type(cls).__name__

        class MyStruct(tuple, metaclass=MetaMyStructType):

            @property
            def __typesystem_type__(self):
                # Python instance dependent Type
                return Type.fromstring(S) | type(self).__name__

            @classmethod
            def fromobject(cls, obj):
                return cls([getattr(obj, m.name) for m in cls.__typesystem_type__])

            def __getattr__(self, name):
                typ = self.__typesystem_type__
                index = typ.get_field_position(name)
                return self[index]

        x, y, z = 123, 2, 3
        s = MyStruct((x, y, z))

        assert type(s).__typesystem_type__ == s.__typesystem_type__
        assert type(s).__typesystem_type__.annotation() != s.__typesystem_type__.annotation()

        # Get the members of struct given by value and reference

        @jit('T(S)', 'T(S*)')
        def get_x(s): return s.x

        @jit('T(S)', 'T(S*)')
        def get_y(s): return s.y

        @jit('T(S)', 'T(S*)')
        def get_z(s): return s.z

        assert get_x(s) == x
        assert get_y(s) == y
        assert get_z(s) == z

        # Set the members of struct given by reference
        # Manage memory of the arrays with struct values

        rcalloc, rfree = map(external, memory_managers['cstdlib'])

        @jit('S* allocate_S(size_t i)')
        def allocate_S(i):
            return rcalloc(i, sizeof(nb_S))

        @jit('void free(void*)')
        def free(s):
            rfree(s)

        @jit('void set_x(S*, T)')
        def set_x(s, x):
            s.x = x

        @jit('void set_y(S*, T)')
        def set_y(s, y):
            s.y = y

        @jit('void set_z(S*, T)')
        def set_z(s, z):
            s.z = z

        p = allocate_S(1)

        set_x(p, x)
        assert get_x(p) == x

        set_y(p, y)
        assert get_y(p) == y

        set_z(p, z)
        assert get_z(p) == z

        free(p)

        @jit('size_t sizeof_S(S)', 'size_t sizeof_S(S*)', 'size_t sizeof_T(T)')
        def sizeof_S(s):
            return sizeof(s)

        assert sizeof_S(s) == sizeof_S(nb_T(0)) * 3
        assert sizeof_S(p) == 8

        # Pointer arithmetics and getitem

        @jit('void(S*, int64)')
        def init_S(s, n):
            s.x = 8
            for i in range(n):
                si = s + i  # pointer arithmetics
                si.x = i
                si.y = i*10
                si.z = i*20

        @jit('void(S*, int64)')
        def init_S2(s, n):
            s.x = 8
            for i in range(n):
                # `s[i]` is equivalent to `s + i`! That is, the result
                # of `s[i]` is a pointer value `S*` rather than a
                # value of `S`.
                si = s[i]
                si.x = i*5
                si.y = i*15
                si.z = i*25

        @jit('S*(S*)')
        def next_S(s):
            return s + 1

        n = 5
        p = allocate_S(n)
        init_S(p, n)

        r = p
        for i in range(n):
            assert (get_x(r), get_y(r), get_z(r)) == (i, i*10, i*20)
            r = next_S(r)

        init_S2(p, n)

        r = p
        for i in range(n):
            assert (get_x(r), get_y(r), get_z(r)) == (i*5, i*15, i*25)
            r = next_S(r)

        free(p)


@pytest.mark.parametrize("location", ['local', 'remote'])
@pytest.mark.parametrize("T", ['int64', 'int32', 'int16', 'int8', 'float32', 'float64'])
def test_struct_pointer_members(ljit, rjit, location, T):
    jit = rjit if location == 'remote' else ljit

    index_t = 'int32'
    S = '{T* data, index_t size}'

    with Type.alias(T=T, S=S, index_t=index_t):
        Sp = S + '*'
        nb_S = Type.fromstring(S).tonumba()
        nb_Sp = Type.fromstring(Sp).tonumba()
        nb_T = Type.fromstring(T).tonumba()

        rcalloc, rfree = map(external, memory_managers['cstdlib'])

        @jit('S* allocate_S(size_t i)')
        def allocate_S(i):
            raw = rcalloc(i, sizeof(nb_S))
            s = cast(raw, nb_Sp)     # cast `void*` to `S*`
            # s = raw.cast(Sp)       # cast `void*` to `S*`
            s.size = 0
            return s

        @jit('void free(void*)')
        def free(s):
            rfree(s)

        @jit('index_t getsize(S*)')
        def getsize(s):
            return s.size

        @jit('T getitem(S*, index_t)')
        def getitem(s, i):
            if i >= 0 and i < s.size:
                return s.data[i]
            return 0  # TODO: return some null value or raise exception

        @jit('void resize_S(S*, index_t)')
        def resize_S(s, size):
            if s.size == size:
                return
            if s.size != 0:
                rfree(s.data)
                s.size = 0
            if s.size == 0 and size > 0:
                raw = rcalloc(size, sizeof(nb_T))
                s.data = raw
                s.size = size

        s = allocate_S(1)
        assert getsize(s) == 0
        assert getitem(s, 0) == 0

        n = 10
        resize_S(s, n)
        assert getsize(s) == n

        n = 5
        resize_S(s, n)
        assert getsize(s) == n

        @jit('void fill_S(S*, T)')
        def fill_S(s, v):
            for i in range(s.size):
                s.data[i] = v

        fill_S(s, 123)
        assert getitem(s, 0) == 123

        @jit('void range_S(S*)')
        def range_S(s):
            for i in range(s.size):
                s.data[i] = i

        range_S(s)

        for i in range(getsize(s)):
            assert getitem(s, i) == i

        resize_S(s, 0)  # deallocate data
        free(s)         # deallocate struct


@pytest.mark.parametrize("location", ['local', 'remote'])
@pytest.mark.parametrize("T", ['int64', 'int32', 'int16', 'int8', 'float32', 'float64'])
def test_struct_double_pointer_members(ljit, rjit, location, T):
    jit = rjit if location == 'remote' else ljit

    index_t = 'int32'
    S = '{T** data, index_t length, index_t size}'
    S1 = '{T* data, index_t size}'

    with Type.alias(T=T, S=S, S1=S1, index_t=index_t):
        Sp = S + '*'
        S1p = S1 + '*'
        Tp = T + '*'
        Tpp = T + '**'
        nb_S = Type.fromstring(S).tonumba()
        nb_Sp = Type.fromstring(Sp).tonumba()
        nb_S1 = Type.fromstring(S1).tonumba()
        nb_S1p = Type.fromstring(S1p).tonumba()
        nb_T = Type.fromstring(T).tonumba()
        nb_Tp = Type.fromstring(Tp).tonumba()
        nb_Tpp = Type.fromstring(Tpp).tonumba()

        rcalloc, rfree = map(external, memory_managers['cstdlib'])

        @jit('S* allocate_S(size_t length, size_t size)')
        def allocate_S(length, size):
            s = rcalloc(1, sizeof(nb_S)).cast(nb_Sp)
            s.data = rcalloc(length, sizeof(nb_Tp)).cast(nb_Tpp)
            s.length = length
            for i in range(length):
                s.data[i] = rcalloc(size, sizeof(nb_T)).cast(nb_Tp)
            s.size = size
            return s

        @jit('void deallocate_S(S*)')
        def deallocate_S(s):
            if s.length > 0:
                for i in range(s.length):
                    rfree(s.data[i])
                rfree(s.data)
                rfree(s)

        @jit('S1* viewitem(S*, index_t)')
        def viewitem(s, i):
            s1 = rcalloc(1, sizeof(nb_S1)).cast(nb_S1p)
            if i >= 0 and i < s.size:
                s1.size = s.size
                s1.data = s.data[i]
            else:
                s1.size = 0
            return s1

        @jit('T getitem(S*, index_t, index_t)')
        def getitem(s, col, row):
            if col >= 0 and col < s.length:
                if row >= 0 and row < s.size:
                    return s.data[col][row]
            return 0

        @jit('void free(S1*)')
        def free(s1):
            rfree(s1)

        @jit('void fill1(S1*, T)')
        def fill1(s1, v):
            for i in range(s1.size):
                s1.data[i] = v

        @jit('void range1(S1*)')
        def range1(s1):
            for i in range(s1.size):
                s1.data[i] = i

        m, n = 4, 5
        s = allocate_S(m, n)

        # initialize
        expected = []
        for col in range(m):
            s1 = viewitem(s, col)
            if col % 2:
                expected.extend(n * [23+col*10])
                fill1(s1, 23+col*10)
            else:
                expected.extend(range(n))
                range1(s1)
            free(s1)

        # check
        actual = []
        for col in range(m):
            for row in range(n):
                actual.append(getitem(s, col, row))
        deallocate_S(s)

        assert expected == actual
