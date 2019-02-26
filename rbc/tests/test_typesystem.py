import pytest
from rbc.typesystem import Type


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


def test_fromstring():
    assert Type.fromstring('void') == Type()
    assert Type.fromstring('') == Type()
    assert Type.fromstring('none') == Type()
    assert Type.fromstring('i') == Type('int32')
    assert Type.fromstring('i*') == Type(Type('int32'), '*')
    assert Type.fromstring('*') == Type(Type(), '*')
    assert Type.fromstring('void*') == Type(Type(), '*')
    assert Type.fromstring('{i,j}') == Type(Type('int32'), Type('j'))
    assert Type.fromstring('i(j)') == Type(Type('int32'), (Type('j'), ))
    assert Type.fromstring('i(j , k)') == Type(Type('int32'),
                                               (Type('j'), Type('k')))
    assert Type.fromstring('  (j , k) ') == Type(Type(),
                                                 (Type('j'), Type('k')))
    assert Type.fromstring('void(j,k)') == Type(Type(),
                                                (Type('j'), Type('k')))

    with pytest.raises(ValueError, match=r'failed to find lparen index in'):
        Type.fromstring('a)')

    with pytest.raises(ValueError, match=r'failed to comma-split'):
        Type.fromstring('a((b)')

    with pytest.raises(ValueError, match=r'failed to comma-split'):
        Type.fromstring('a((b)')

    with pytest.raises(ValueError, match=r'mismatching curly parenthesis in'):
        Type.fromstring('ab}')


def test_is_properties():
    t = Type()
    assert t.is_ok and t.is_void
    t = Type('i')
    assert t.is_ok and t.is_atomic
    t = Type('ij')
    assert t.is_ok and t.is_atomic
    t = Type.fromstring('ij')
    assert t.is_ok and t.is_atomic
    t = Type.fromstring('i,j')
    assert t.is_ok and t.is_atomic  # !
    t = Type.fromstring('i  *')
    assert t.is_ok and t.is_pointer
    t = Type.fromstring('*')
    assert t.is_ok and t.is_pointer
    t = Type.fromstring('i* * ')
    assert t.is_ok and t.is_pointer
    t = Type.fromstring('i(j)')
    assert t.is_ok and t.is_function
    t = Type.fromstring('(j)')
    assert t.is_ok and t.is_function
    t = Type.fromstring('()')
    assert t.is_ok and t.is_function
    t = Type.fromstring('{i, j}')
    assert t.is_ok and t.is_struct

    with pytest.raises(ValueError,
                       match=r'attempt to create an invalid type object from'):
        Type('a', 'b')


def test_tostring():

    def tostr(a):
        return Type.fromstring(a).tostring()

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
        return Type.fromstring(a).tostring()

    assert tostr('a') == 'a'
    assert tostr('int32') == 'int32'
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

    assert tostr('str') == 'string'
    assert tostr('string') == 'string'

    assert tostr('i(i*, i15)') == 'int32(int32*, int15)'
    assert tostr('{i,d,c, bool,f,str*}') \
        == '{int32, float64, complex64, bool, float32, string*}'


def test_toctypes():
    import ctypes

    def toctypes(a):
        return Type.fromstring(a).toctypes()

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
        return Type.fromstring(a)

    def fromctypes(t):
        return Type.fromctypes(t)

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


def test_tonumba():
    import numba as nb

    def tonumba(a):
        return Type.fromstring(a).tonumba()

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
    assert tonumba('()') == nb.void(nb.void)
    assert tonumba('d(i64, f)') == nb.double(nb.int64, nb.float_)
    # assert tonumba('{i,d}')  # numba does not support C struct


def test_fromnumba():
    import numba as nb

    def fromstr(a):
        return Type.fromstring(a)

    def fromnumba(t):
        return Type.fromnumba(t)

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
