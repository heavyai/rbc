try:
    import numba as nb
    nb_NA_message = None
except ImportError as msg:
    nb = None
    nb_NA_message = str(msg)

try:
    import numpy as np
    np_NA_message = None
except ImportError as msg:
    np = None
    np_NA_message = str(msg)

try:
    import numpy as np
except ImportError:
    np = None

import pytest
from rbc.typesystem import Type, get_signature
from rbc.utils import get_datamodel
from rbc.targetinfo import TargetInfo

target_info = TargetInfo.host()


def Type_fromstring(s):
    return Type.fromstring(s, target_info)


def Type_fromobject(s):
    return Type.fromobject(s, target_info)


def Type_fromcallable(s):
    return Type.fromcallable(s, target_info)


def Type_fromvalue(s):
    return Type.fromvalue(s, target_info)


def test_findparen():
    from rbc.typesystem import _findparen as findparen
    assert findparen('a(b)') == 1
    assert findparen('a(b, c())') == 1
    assert findparen('a(b, c(d))') == 1
    assert findparen('a(b(a), c(f(), d))') == 1
    assert findparen('a()(b)') == 3
    assert findparen('{a(),c()}(b)') == 9


def test_commasplit():
    from rbc.typesystem import _commasplit as commasplit
    assert '^'.join(commasplit('a')) == 'a'
    assert '^'.join(commasplit('a, b')) == 'a^b'
    assert '^'.join(commasplit('a, b  , c')) == 'a^b^c'
    assert '^'.join(commasplit('a, (b, e)  , c')) == 'a^(b, e)^c'
    assert '^'.join(commasplit('a, (b, (e,f ,g{h, j}))  , c')) \
        == 'a^(b, (e,f ,g{h, j}))^c'
    assert '^'.join(commasplit('(a, b)')) == '(a, b)'
    assert '^'.join(commasplit('{a, b}')) == '{a, b}'
    assert '^'.join(commasplit('(a, b) , {d, e}')) == '(a, b)^{d, e}'
    assert '^'.join(commasplit('(a[:, :])')) == '(a[:, :])'
    assert '^'.join(commasplit('a[:, :], b[:, :, :]')) == 'a[:, :]^b[:, :, :]'


def test_fromstring():
    assert Type_fromstring('void') == Type()
    assert Type_fromstring('') == Type()
    assert Type_fromstring('none') == Type()
    assert Type_fromstring('i') == Type('int32')
    assert Type_fromstring('i*') == Type(Type('int32'), '*')
    assert Type_fromstring('*') == Type(Type(), '*')
    assert Type_fromstring('void*') == Type(Type(), '*')
    assert Type_fromstring('{i,j}') == Type(Type('int32'), Type('j'))
    assert Type_fromstring('i(j)') == Type(Type('int32'), (Type('j'), ))
    assert Type_fromstring('i(j , k)') == Type(Type('int32'),
                                               (Type('j'), Type('k')))
    assert Type_fromstring('  (j , k) ') == Type(Type(),
                                                 (Type('j'), Type('k')))
    assert Type_fromstring('void(j,k)') == Type(Type(),
                                                (Type('j'), Type('k')))

    assert Type_fromstring('i a') == Type('int32', name='a')
    assert Type_fromstring('i* a') == Type(Type('int32'), '*', name='a')
    assert Type_fromstring('{i,j} a') == Type(Type('int32'), Type('j'),
                                              name='a')
    assert Type_fromstring('i(j) a') == Type(Type('int32'), (Type('j'),),
                                             name='a')
    assert Type_fromstring('i a*') == Type(Type('int32', name='a'), '*')
    assert Type_fromstring('{i a,j b} c') == Type(Type('int32', name='a'),
                                                  Type('j', name='b'),
                                                  name='c')

    with pytest.raises(ValueError, match=r'failed to find lparen index in'):
        Type_fromstring('a)')

    with pytest.raises(ValueError, match=r'failed to comma-split'):
        Type_fromstring('a((b)')

    with pytest.raises(ValueError, match=r'failed to comma-split'):
        Type_fromstring('a((b)')

    with pytest.raises(ValueError, match=r'mismatching curly parenthesis in'):
        Type_fromstring('ab}')


def test_is_properties():
    t = Type()
    assert t._is_ok and t.is_void
    t = Type('i')
    assert t._is_ok and t.is_atomic
    t = Type('ij')
    assert t._is_ok and t.is_atomic
    t = Type_fromstring('ij')
    assert t._is_ok and t.is_atomic
    t = Type_fromstring('i,j')
    assert t._is_ok and t.is_atomic  # !
    t = Type_fromstring('i  *')
    assert t._is_ok and t.is_pointer
    t = Type_fromstring('*')
    assert t._is_ok and t.is_pointer
    t = Type_fromstring('i* * ')
    assert t._is_ok and t.is_pointer
    t = Type_fromstring('i(j)')
    assert t._is_ok and t.is_function
    t = Type_fromstring('(j)')
    assert t._is_ok and t.is_function
    t = Type_fromstring('()')
    assert t._is_ok and t.is_function
    t = Type_fromstring('{i, j}')
    assert t._is_ok and t.is_struct

    with pytest.raises(ValueError,
                       match=r'attempt to create an invalid Type object from'):
        Type('a', 'b')


def test_tostring():

    def tostr(a):
        return Type_fromstring(a).tostring()

    assert tostr('a') == 'a'
    assert tostr('()') == 'void(void)'
    assert tostr('(a,b,c)') == 'void(a, bool, complex64)'
    assert tostr('f  (   )') == 'float32(void)'
    assert tostr('f[a,c]  (   )') == 'f[a,c](void)'
    assert tostr(' f,g ()') == 'f,g(void)'
    assert tostr('a * ') == 'a*'
    assert tostr(' a  * ( b * , c * )  ') == 'a*(bool*, complex64*)'
    assert tostr('{a}') == '{a}'
    assert tostr('{a  ,b}') == '{a, bool}'
    assert tostr('{{a,c} ,b}') == '{{a, complex64}, bool}'
    assert tostr('*') == 'void*'
    assert tostr('void *') == 'void*'
    assert tostr('*(*,{*,*})') == 'void*(void*, {void*, void*})'


def test_normalize():

    def tostr(a):
        return Type_fromstring(a).tostring()

    assert tostr('a') == 'a'
    assert tostr('int32') == 'int32'
    assert tostr('int32_t') == 'int32'
    assert tostr('int') == 'int32'
    assert tostr('i') == 'int32'
    assert tostr('i35') == 'int35'
    assert tostr('byte') == 'int8'
    assert tostr('ubyte') == 'uint8'

    assert tostr('uint32') == 'uint32'
    assert tostr('uint') == 'uint32'
    assert tostr('u') == 'uint32'
    assert tostr('u35') == 'uint35'
    assert tostr('unsigned int') == 'uint32'

    assert tostr('float32') == 'float32'
    assert tostr('f32') == 'float32'
    assert tostr('f') == 'float32'
    assert tostr('float') == 'float32'
    assert tostr('float64') == 'float64'
    assert tostr('double') == 'float64'
    assert tostr('d') == 'float64'

    assert tostr('complex32') == 'complex32'
    assert tostr('c32') == 'complex32'
    assert tostr('c') == 'complex64'
    assert tostr('complex') == 'complex64'

    assert tostr('') == 'void'
    assert tostr('bool') == 'bool'
    assert tostr('b') == 'bool'
    assert tostr('_Bool') == 'bool'

    assert tostr('str') == 'string'
    assert tostr('string') == 'string'

    assert tostr('i(i*, i15)') == 'int32(int32*, int15)'
    assert tostr('{i,d,c, bool,f,str*}') \
        == '{int32, float64, complex64, bool, float32, string*}'

    datamodel = get_datamodel()
    if datamodel == 'LP64':
        assert tostr('l') == 'int64'
        assert tostr('long') == 'int64'
        assert tostr('long long') == 'int64'
        assert tostr('unsigned long') == 'uint64'
        assert tostr('short') == 'int16'
        assert tostr('unsigned short') == 'uint16'
        assert tostr('ssize_t') == 'int64'
        assert tostr('size_t') == 'uint64'
        assert tostr('c_size_t') == 'uint64'
        assert tostr('std::size_t') == 'uint64'
        assert tostr('long double') == 'float128'
        assert tostr('byte') == 'int8'
        assert tostr('unsigned byte') == 'uint8'
        assert tostr('signed char') == 'int8'
        assert tostr('unsigned char') == 'uint8'
        assert tostr('wchar_t') == 'char32'
        assert tostr('char32') == 'char32'
        assert tostr('signed') == 'int32'
        assert tostr('unsigned') == 'uint32'
    elif datamodel == 'LLP64':
        assert tostr('l') == 'int32'
        assert tostr('long') == 'int32'
        assert tostr('long long') == 'int64'
        assert tostr('unsigned long') == 'uint32'
        assert tostr('short') == 'int16'
        assert tostr('unsigned short') == 'uint16'
        assert tostr('ssize_t') == 'int64'
        assert tostr('size_t') == 'uint64'
        assert tostr('c_size_t') == 'uint64'
        assert tostr('std::size_t') == 'uint64'
        assert tostr('long double') == 'float64'
        assert tostr('byte') == 'int8'
        assert tostr('unsigned byte') == 'uint8'
        assert tostr('signed char') == 'int8'
        assert tostr('unsigned char') == 'uint8'
        assert tostr('wchar_t') == 'char16'
        assert tostr('char32') == 'char32'
        assert tostr('signed') == 'int32'
        assert tostr('unsigned') == 'uint32'
    else:
        raise NotImplementedError('tests for datamodel=`%s`' % (datamodel))


def test_toctypes():
    import ctypes

    def toctypes(a):
        return Type_fromstring(a).toctypes()

    assert toctypes('bool') == ctypes.c_bool
    assert toctypes('i8') == ctypes.c_int8
    assert toctypes('i32') == ctypes.c_int32
    assert toctypes('u32') == ctypes.c_uint32
    assert toctypes('double') == ctypes.c_double
    assert toctypes('float') == ctypes.c_float
    assert toctypes('char') == ctypes.c_char
    assert toctypes('char8') == ctypes.c_char
    assert toctypes('char*') == ctypes.c_char_p
    assert toctypes('wchar') == ctypes.c_wchar
    assert toctypes('wchar*') == ctypes.c_wchar_p
    assert toctypes('*') == ctypes.c_void_p
    assert toctypes('void*') == ctypes.c_void_p
    assert toctypes('void') is None
    assert toctypes('i(i, double)') \
        == ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_int32, ctypes.c_double)
    s = toctypes('{i, double}')
    assert issubclass(s, ctypes.Structure)
    assert s._fields_ == [('f0', ctypes.c_int32), ('f1', ctypes.c_double)]


def test_fromctypes():
    import ctypes

    def fromstr(a):
        return Type_fromstring(a)

    def fromctypes(t):
        return Type.fromctypes(t, target_info)

    assert fromctypes(ctypes.c_char_p) == fromstr('char*')
    assert fromctypes(ctypes.c_wchar_p) == fromstr('wchar*')
    assert fromctypes(ctypes.c_int8) == fromstr('i8')
    assert fromctypes(ctypes.c_uint8) == fromstr('u8')
    assert fromctypes(ctypes.c_uint64) == fromstr('u64')
    assert fromctypes(ctypes.c_float) == fromstr('f32')
    assert fromctypes(ctypes.c_double) == fromstr('double')
    assert fromctypes(ctypes.c_void_p) == fromstr('*')
    assert fromctypes(None) == fromstr('void')

    class mystruct(ctypes.Structure):
        _fields_ = [('f0', ctypes.c_int32), ('f1', ctypes.c_double)]

    assert fromctypes(mystruct) == fromstr('{i32, double}')
    assert fromctypes(ctypes.POINTER(ctypes.c_float)) == fromstr('float*')
    assert fromctypes(ctypes.CFUNCTYPE(ctypes.c_float, ctypes.c_int)) \
        == fromstr('f(i)')


@pytest.mark.skipif(nb is None, reason=nb_NA_message)
def test_tonumba():
    def tonumba(a):
        return Type_fromstring(a).tonumba()

    assert tonumba('void') == nb.void
    assert tonumba('bool') == nb.boolean
    assert tonumba('int8') == nb.int8
    assert tonumba('int16') == nb.int16
    assert tonumba('int32') == nb.int32
    assert tonumba('int64') == nb.int64
    assert tonumba('uint8') == nb.uint8
    assert tonumba('uint16') == nb.uint16
    assert tonumba('uint32') == nb.uint32
    assert tonumba('uint64') == nb.uint64
    assert tonumba('float') == nb.float32
    assert tonumba('double') == nb.float64
    assert tonumba('complex') == nb.complex64
    assert tonumba('complex128') == nb.complex128
    assert tonumba('double*') == nb.types.CPointer(nb.float64)
    assert tonumba('()') == nb.void()
    assert tonumba('d(i64, f)') == nb.double(nb.int64, nb.float_)
    # assert tonumba('{i,d}')  # numba does not support C struct


@pytest.mark.skipif(nb is None, reason=nb_NA_message)
def test_fromnumba():
    import numba as nb

    def fromstr(a):
        return Type_fromstring(a)

    def fromnumba(t):
        return Type.fromnumba(t, target_info)

    assert fromnumba(nb.void) == fromstr('void')
    assert fromnumba(nb.boolean) == fromstr('bool')
    assert fromnumba(nb.int8) == fromstr('int8')
    assert fromnumba(nb.int16) == fromstr('int16')
    assert fromnumba(nb.int32) == fromstr('int32')
    assert fromnumba(nb.int64) == fromstr('int64')
    assert fromnumba(nb.uint8) == fromstr('uint8')
    assert fromnumba(nb.uint16) == fromstr('uint16')
    assert fromnumba(nb.uint32) == fromstr('uint32')
    assert fromnumba(nb.uint64) == fromstr('uint64')
    assert fromnumba(nb.float_) == fromstr('float32')
    assert fromnumba(nb.double) == fromstr('float64')
    assert fromnumba(nb.complex64) == fromstr('complex64')
    assert fromnumba(nb.complex128) == fromstr('complex128')
    assert fromnumba(nb.types.CPointer(nb.float64)) == fromstr('double*')
    assert fromnumba(nb.double(nb.int64, nb.float_)) == fromstr('d(i64, f)')


@pytest.mark.skipif(np is None, reason='NumPy is not available')
def test_fromnumpy():

    def fromstr(a):
        return Type_fromstring(a)

    def fromnumpy(t):
        return Type.fromnumpy(t, target_info)

    assert fromnumpy(np.void) == fromstr('void')
    assert fromnumpy(np.bool_) == fromstr('bool')
    assert fromnumpy(np.bytes_) == fromstr('bytes')
    assert fromnumpy(np.complex64) == fromstr('complex64')
    assert fromnumpy(np.complex128) == fromstr('complex128')
    assert fromnumpy(np.datetime64) == fromstr('datetime64')
    assert fromnumpy(np.float16) == fromstr('float16')
    assert fromnumpy(np.float32) == fromstr('float32')
    assert fromnumpy(np.float64) == fromstr('float64')
    assert fromnumpy(np.double) == fromstr('float64')
    assert fromnumpy(np.int8) == fromstr('int8')
    assert fromnumpy(np.int16) == fromstr('int16')
    assert fromnumpy(np.int32) == fromstr('int32')
    assert fromnumpy(np.int64) == fromstr('int64')
    assert fromnumpy(np.longlong) == fromstr('int64')
    assert fromnumpy(np.object_) == fromstr('object')
    assert fromnumpy(np.str_) == fromstr('str')
    assert fromnumpy(np.timedelta64) == fromstr('timedelta64')
    assert fromnumpy(np.uint8) == fromstr('uint8')
    assert fromnumpy(np.uint16) == fromstr('uint16')
    assert fromnumpy(np.uint32) == fromstr('uint32')
    assert fromnumpy(np.uint64) == fromstr('uint64')
    assert fromnumpy(np.ulonglong) == fromstr('uint64')

    if hasattr(np, 'float128'):
        assert fromnumpy(np.float128) == fromstr('float128')
    if hasattr(np, 'complex256'):
        assert fromnumpy(np.complex256) == fromstr('complex256')


def test_fromcallable():

    def foo(a: int, b: float) -> int:
        pass

    assert Type_fromcallable(foo) == Type_fromstring('i64(i64,d)')

    def foo(a: 'int32', b):  # noqa: F821
        pass

    assert Type_fromcallable(foo) == Type_fromstring('void(i32,<type of b>)')

    with pytest.raises(
            ValueError,
            match=(r'constructing Type instance from'
                   r' a lambda function is not supported')):
        Type_fromcallable(lambda a: a)

    with pytest.raises(
            ValueError,
            match=r'callable argument kind must be positional'):
        def foo(*args): pass
        Type_fromcallable(foo)


def test_fromvalue():
    assert Type_fromvalue(1) == Type_fromstring('i64')
    assert Type_fromvalue(1.0) == Type_fromstring('f64')
    assert Type_fromvalue(1j) == Type_fromstring('c128')
    assert Type_fromvalue("123".encode()) == Type_fromstring('char*')
    assert Type_fromvalue("123") == Type_fromstring('string')
    x = np.dtype(np.float64).type(3.0)
    assert Type_fromvalue(x) == Type_fromstring('float64')
    y = np.dtype(np.complex64).type((1+2j))
    assert Type_fromvalue(y) == Type_fromstring('complex64')


def test_fromobject():
    import ctypes
    assert Type_fromobject('i8') == Type_fromstring('i8')
    assert Type_fromobject(int) == Type_fromstring('i64')
    assert Type_fromobject(ctypes.c_int16) == Type_fromstring('i16')
    if nb is not None:
        assert Type_fromobject(nb.int16) == Type_fromstring('i16')
    if np is not None:
        assert Type_fromobject(np.int32) == Type_fromstring('i32')
        assert Type_fromobject(np.complex64) == Type_fromstring('complex64')

    def foo():
        pass

    assert Type_fromobject(foo) == Type_fromstring('void(void)')


def test_mangling():
    def check(s):
        t1 = Type_fromstring(s)
        m = t1.mangle()
        try:
            t2 = Type.demangle(m)
        except Exception:
            print('subject: s=`%s`, t1=`%s`, m=`%s`' % (s, t1, m))
            raise
        assert t1 == t2, repr((t1, m, t2))

    atomic_types = ['void', 'bool', 'char8', 'char16', 'char32',
                    'int8', 'int16', 'int32', 'int64', 'int128',
                    'uint8', 'uint16', 'uint32', 'uint64', 'uint128',
                    'f16', 'f32', 'f64', 'f128',
                    'complex32', 'complex64', 'complex128', 'complex256']
    random_types = ['i8', 'bool', 'f', 'd', '{f}', '{f,d}', '{{f},d}',
                    '{f,{d}}', '{{f},{d}}', '{{{{f}}}}', '()', 'f(d)',
                    'f(())', 'f(d(f))', 'f(d)()', 'f(d)(f(d,d))',
                    '{f}()', '{f,d}({f},f(d,d,d))']
    unknown_types = ['a', 'a*', 'a()', '(a)', 'a(a)', '{a}', '({a})', '{a,a}',
                     'foo', 'bar123', 'V', 'VVV', '_abc_', '_', 'A', 'K', 'P']
    for s in atomic_types + random_types + unknown_types:
        check(s)
        check(s+'*')
        check('{'+s+'}')
        check(s+'('+s+')')
        check('{'+s+','+s+'}')
        check('{'+s+'*,'+s+'}')
        check(s+'('+s+','+s+')')
        check('('+s+','+s+')')
        check(s+'({'+s+'})')
        check(s+'({'+s+'}, '+s+')')
        check(s+'('+s+',{'+s+'})')
        check(s+'('+s+',{'+s+'},'+s+')')
    check('()')


def test_unspecified():
    assert str(Type_fromstring('unknown(_0,_1)')) == 'unknown(_0, _1)'


def test_annotation():
    t = Type_fromstring('int foo| a = 1')
    assert t.annotation() == dict(a='1')
    assert t[0] == 'int32'

    def tostr(a):
        return Type_fromstring(a).tostring()

    assert tostr('int foo| a = 1') == 'int32 foo | a=1'
    assert tostr('int foo| a = 1 | b') == 'int32 foo | a=1 | b'
    assert tostr('int foo| a = 1 | a = 2') == 'int32 foo | a=2'

    assert tostr('int| a = 1') == 'int32 | a=1'
    assert tostr('int*| a = 1') == 'int32* | a=1'
    assert tostr('{int, int}| a = 1') == '{int32, int32} | a=1'
    assert (tostr('{int|a=1, int|a=2}| a = 3')
            == '{int32 | a=1, int32 | a=2} | a=3')
    assert tostr('int foo|') == 'int32 foo'
    assert tostr('int foo|a') == 'int32 foo | a'
    assert tostr('int foo|=1') == 'int32 foo | =1'

    t = Type_fromstring('int')
    assert (t | 'a').tostring() == 'int32 | a'
    assert (t | dict(b=1, c=2)).tostring() == 'int32 | b=1 | c=2'


@pytest.mark.skipif(np is None, reason=np_NA_message)
def test_get_signature_ufunc():

    # Make sure that all get_signature can be applied to all numpy
    # ufuncs
    for name, func in np.__dict__.items():
        if isinstance(func, np.ufunc):
            get_signature(func)

    sig = get_signature(np.trunc)
    assert len(sig.parameters) == 1

    sig = get_signature(np.modf)
    assert len(sig.parameters) == 3
