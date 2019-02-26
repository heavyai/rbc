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

    assert Type.fromstring('i') == Type('i')
    assert Type.fromstring('i*') == Type(Type('i'), '*')
    assert Type.fromstring('*') == Type(Type(), '*')
    assert Type.fromstring('void*') == Type(Type(), '*')
    assert Type.fromstring('{i,j}') == Type(Type('i'), Type('j'))
    assert Type.fromstring('i(j)') == Type(Type('i'), (Type('j'), ))
    assert Type.fromstring('i(j , k)') == Type(Type('i'),
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
    assert tostr('(a,b,c)') == 'void(a, b, c)'
    assert tostr('f  (   )') == 'f(void)'
    assert tostr('f[a,c]  (   )') == 'f[a,c](void)'
    assert tostr(' f,g ()') == 'f,g(void)'
    assert tostr('a * ') == 'a*'
    assert tostr(' a  * ( b * , c * )  ') == 'a*(b*, c*)'
    assert tostr('{a}') == '{a}'
    assert tostr('{a  ,b}') == '{a, b}'
    assert tostr('{{a,c} ,b}') == '{{a, c}, b}'
    assert tostr('*') == 'void*'
    assert tostr('void *') == 'void*'
    assert tostr('*(*,{*,*})') == 'void*(void*, {void*, void*})'


def test_normalize():
    def tostr(a):
        return Type.fromstring(a).normalize().tostring()
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
