"""Provides Type structures.
"""
# Author: Pearu Peterson
# Created: February 2019


import re
import ctypes
import inspect
try:
    import numba as nb
except ImportError as msg:
    nb = None
    nb_NA_message = str(msg)


class TypeParseError(Exception):
    """Failure to parse type definition
    """


def _findparen(s):
    """Find the index of left parenthesis that matches with the one at the
    end of a string.

    Used internally.
    """
    j = s.find(')')
    assert j >= 0, repr((j, s))
    if j == len(s) - 1:
        i = s.find('(')
        if i < 0:
            raise TypeParseError('failed to find lparen index in `%s`' % s)
        return i
    i = s.rfind('(', 0, j)
    if i < 0:
        raise TypeParseError('failed to find lparen index in `%s`' % s)
    t = s[:i] + '_'*(j-i+1) + s[j+1:]
    assert len(t) == len(s), repr((t, s))
    return _findparen(t)


def _commasplit(s):
    """Split a comma-separated items taking into account parenthesis.

    Used internally.
    """
    lst = s.split(',')
    ac = ''
    p1, p2 = 0, 0
    rlst = []
    for i in lst:
        p1 += i.count('(') - i.count(')')
        p2 += i.count('{') - i.count('}')
        if p1 == p2 == 0:
            rlst.append((ac + ',' + i if ac else i).strip())
            ac = ''
        else:
            ac = ac + ',' + i if ac else i
    if p1 == p2 == 0:
        return rlst
    raise TypeParseError('failed to comma-split `%s`' % s)


_bool_match = re.compile(r'\A(boolean|bool|b)\Z').match
_char_match = re.compile(r'\A(char)\s*(8|16|32|)\Z').match
_string_match = re.compile(r'\A(string|str)\Z').match
_int_match = re.compile(r'\A(int|i)\s*(\d*)\Z').match
_uint_match = re.compile(r'\A(uint|u|unsigned\s*int)\s*(\d*)\Z').match
_float_match = re.compile(r'\A(float|f)\s*(16|32|64|128|256|)\Z').match
_double_match = re.compile(r'\A(double|d)\Z').match
_complex_match = re.compile(r'\A(complex|c)\s*(16|32|64|128|256|512|)\Z').match

# Initialize type maps
_ctypes_imap = {ctypes.c_void_p: 'void*', None: 'void', ctypes.c_bool: 'bool',
                ctypes.c_char_p: 'char8*', ctypes.c_wchar_p: 'char32*'}
_ctypes_char_map = {}
_ctypes_int_map = {}
_ctypes_uint_map = {}
_ctypes_float_map = {}
_ctypes_complex_map = {}
for _k, _m, _lst in [
        ('char', _ctypes_char_map, ['c_char', 'c_wchar']),
        ('int', _ctypes_int_map,
         ['c_int8', 'c_int16', 'c_int32', 'c_int64', 'c_int',
          'c_long', 'c_longlong', 'c_byte', 'c_short', 'c_ssize_t']),
        ('uint', _ctypes_uint_map,
         ['c_uint8', 'c_uint16', 'c_uint32', 'c_uint64', 'c_uint',
          'c_ulong', 'c_ulonglong', 'c_ubyte', 'c_ushort', 'c_size_t']),
        ('float', _ctypes_float_map,
         ['c_float', 'c_double', 'c_longdouble'])
]:
    for _n in _lst:
        _t = getattr(ctypes, _n, None)
        if _t is not None:
            _b = ctypes.sizeof(_t) * 8
            if _b not in _m:
                _m[_b] = _t
            _ctypes_imap[_t] = _k + str(_b)

if nb is not None:
    _numba_imap = {nb.void: 'void', nb.boolean: 'bool'}
    _numba_char_map = {}
    _numba_int_map = {}
    _numba_uint_map = {}
    _numba_float_map = {}
    _numba_complex_map = {}
    for _k, _m, _lst in [
            ('int', _numba_int_map,
             ['int8', 'int16', 'int32', 'int64', 'intc', 'int_', 'intp',
              'long_', 'longlong', 'short', 'char']),
            ('uint', _numba_uint_map,
             ['uint8', 'uint16', 'uint32', 'uint64', 'uintc', 'uint',
              'uintp', 'ulong', 'ulonglong', 'ushort']),
            ('float', _numba_float_map,
             ['float32', 'float64', 'float_', 'double']),
            ('complex', _numba_complex_map, ['complex64', 'complex128']),
    ]:
        for _n in _lst:
            _t = getattr(nb, _n, None)
            if _t is not None:
                _b = _t.bitwidth
                if _b not in _m:
                    _m[_b] = _t
                _numba_imap[_t] = _k + str(_b)

_python_imap = {int: 'int64', float: 'float64', complex: 'complex128',
                str: 'string', bytes: 'char*'}

# Data for the mangling algorithm, see mangle/demangle methods.
#
_mangling_suffices = '_V'
_mangling_prefixes = 'PKaA'
_mangling_map = dict(
    void='v', bool='b',
    char8='c', char16='z', char32='w',
    int8='B', int16='s', int32='i', int64='l', int128='q',
    uint8='U', uint16='S', uint32='I', uint64='L', uint128='Q',
    float16='h', float32='f', float64='d', float128='x',
    complex32='H', complex64='F', complex128='D', complex256='X',
    string='t',
)
_mangling_imap = {}
for _k, _v in _mangling_map.items():
    assert _v not in _mangling_imap, repr((_k, _v))
    assert len(_v) == 1, repr((_k, _v))
    _mangling_imap[_v] = _k
# make sure that mangling keys will not conflict with mangling
# operators:
_i = set(_mangling_imap).intersection(_mangling_suffices+_mangling_prefixes)
assert not _i, repr(_i)


class Type(tuple):
    """Represents a type.

    There are five kinds of a types:

      void        - a "no type"
      atomic      e.g. `int32`
      pointer     e.g. `int32`
      struct      e.g. `{int32, int32}`
      function    e.g. `int32(int32, int32)`

    Atomic types are types with names (Type contains a single
    string). All other types (except "no type") are certain
    constructions of atomic types.

    The name content of an atomic type is arbitrary but it cannot be
    empty. For instance, all the following Type instances represent
    atomic types: Type('a'), Type('a long name')

    Parsing types from a string is not fixed to any type system, the
    names of types can be arbitrary.  However, converting the Type
    instances to concrete types such as provided in numpy or numba,
    the following atomic types are defined (the first name corresponds
    to normalized name):

      no type:                    void, none
      bool:                       bool, boolean, b
      8-bit char:                 char8, char
      16-bit char:                char16
      32-bit char:                char32, wchar
      8-bit signed integer:       int8, i8, byte
      16-bit signed integer:      int16, i16
      32-bit signed integer:      int32, i32, int
      64-bit signed integer:      int64, i64, long
      128-bit signed integer:     int128, i128, long long
      8-bit unsigned integer:     uint8, u8, ubyte
      16-bit unsigned integer:    uint16, u16
      32-bit unsigned integer:    uint32, u32, uint, unsigned int
      64-bit unsigned integer:    uint64, u64, ulong, unsigned long
      128-bit unsigned integer:   uint128, u128, ulong long, unsigned long long
      16-bit float:               float16, f16, f2
      32-bit float:               float32, f32, f4, float
      64-bit float:               float64, f64, f8, double
      128-bit float:              float128, f128, long double
      32-bit complex:             complex32, c32
      64-bit complex:             complex64, c64
      128-bit complex:            complex128, c128
      256-bit complex:            complex256, c256
      string:                     string, str

    with the following extensions:

      N-bit signed integer: int<N>, i<N>       for instance: int5, i31
      N-bit unsigned integer: uint<N>, u<N>
      N-bit float: float<N>
      N-bit complex: complex<N>

    """

    def __new__(cls, *args):
        obj = tuple.__new__(cls, args)
        if not obj._is_ok:
            raise ValueError(
                'attempt to create an invalid Type object from `%s`' % (args,))
        return obj

    @property
    def is_void(self):
        return len(self) == 0

    @property
    def is_atomic(self):
        return len(self) == 1 and isinstance(self[0], str)

    @property
    def is_int(self):
        return self.is_atomic and self[0].startswith('int')

    @property
    def is_uint(self):
        return self.is_atomic and self[0].startswith('uint')

    @property
    def is_float(self):
        return self.is_atomic and self[0].startswith('float')

    @property
    def is_complex(self):
        return self.is_atomic and self[0].startswith('complex')

    @property
    def is_string(self):
        return self.is_atomic and self[0] == 'string'

    @property
    def is_bool(self):
        return self.is_atomic and self[0] == 'bool'

    @property
    def is_char(self):
        return self.is_atomic and self[0].startswith('char')

    @property
    def is_pointer(self):
        return len(self) == 2 and isinstance(self[0], Type) \
            and isinstance(self[1], str) and self[1] == '*'

    @property
    def is_struct(self):
        return len(self) > 0 and all(isinstance(s, Type) for s in self)

    @property
    def is_function(self):
        return len(self) == 2 and isinstance(self[0], Type) and \
            isinstance(self[1], tuple) and not isinstance(self[1], Type)

    @property
    def is_complete(self):
        """Return True when the Type instance does not contain unknown types.
        """
        if self.is_atomic:
            return not self[0].startswith('<type of')
        elif self.is_pointer:
            return self[0].is_complete
        elif self.is_struct:
            for m in self:
                if not m.complete:
                    return False
        elif self.is_function:
            if not self[0].is_complete:
                return False
            for a in self[1]:
                if not a.is_complete:
                    return False
        elif self.is_void:
            pass
        else:
            raise NotImplementedError(repr(self))
        return True

    @property
    def _is_ok(self):
        return self.is_void or self.is_atomic or self.is_pointer \
            or self.is_struct or (self.is_function and len(self[1]) > 0)

    def __str__(self):
        if self._is_ok:
            return self.tostring()
        return tuple.__str__(self)

    def tostring(self):
        """Return string representation of a type.
        """
        if self.is_void:
            return 'void'
        if self.is_atomic:
            return self[0]
        if self.is_pointer:
            return self[0].tostring() + '*'
        if self.is_struct:
            return '{' + ', '.join([t.tostring() for t in self]) + '}'
        if self.is_function:
            return self[0].tostring() + '(' + ', '.join(
                a.tostring() for a in self[1]) + ')'
        raise NotImplementedError(repr(self))

    def tonumba(self):
        """Convert Type instance to numba type object.
        """
        if self.is_void:
            return nb.void
        if self.is_int:
            return _numba_int_map.get(int(self[0][3:]))
        if self.is_uint:
            return _numba_uint_map.get(int(self[0][4:]))
        if self.is_float:
            return _numba_float_map.get(int(self[0][5:]))
        if self.is_complex:
            return _numba_complex_map.get(int(self[0][7:]))
        if self.is_bool:
            return nb.boolean
        if self.is_pointer:
            return nb.types.CPointer(self[0].tonumba())
        if self.is_struct:
            return nb.typing.ctypes_utils.from_ctypes(self.toctypes())
        if self.is_function:
            rtype = self[0].tonumba()
            atypes = [t.tonumba() for t in self[1]]
            return rtype(*atypes)
        raise NotImplementedError(repr(self))

    def toctypes(self):
        """Convert Type instance to ctypes type object.
        """
        if self.is_void:
            return None
        if self.is_int:
            return _ctypes_int_map.get(int(self[0][3:]))
        if self.is_uint:
            return _ctypes_uint_map.get(int(self[0][4:]))
        if self.is_float:
            return _ctypes_float_map.get(int(self[0][5:]))
        if self.is_complex:
            return _ctypes_complex_map.get(int(self[0][7:]))
        if self.is_bool:
            return ctypes.c_bool
        if self.is_char:
            return _ctypes_char_map.get(int(self[0][4:]))
        if self.is_pointer:
            if self[0].is_void:
                return ctypes.c_void_p
            if self[0].is_char:
                return getattr(ctypes,
                               _ctypes_char_map.get(
                                   int(self[0][0][4:])).__name__ + '_p')
            return ctypes.POINTER(self[0].toctypes())
        if self.is_struct:
            fields = [('f%s' % i, t.toctypes()) for i, t in enumerate(self)]
            return type('struct%s' % (id(self)),
                        (ctypes.Structure, ),
                        dict(_fields_=fields))
        if self.is_function:
            rtype = self[0].toctypes()
            atypes = [t.toctypes() for t in self[1]]
            return ctypes.CFUNCTYPE(rtype, *atypes)
        raise NotImplementedError(repr(self))

    @classmethod
    def _fromstring(cls, s):
        s = s.strip()
        if s.endswith('*'):       # pointer
            return cls(cls._fromstring(s[:-1]), '*')
        if s.endswith('}'):       # struct
            if not s.startswith('{'):
                raise TypeParseError(
                    'mismatching curly parenthesis in `%s`' % (s))
            return cls(*map(cls._fromstring,
                            _commasplit(s[1:-1].strip())))
        if s.endswith(')'):       # function
            i = _findparen(s)
            if i < 0:
                raise TypeParseError('mismatching parenthesis in `%s`' % (s))
            rtype = cls._fromstring(s[:i])
            atypes = tuple(map(cls._fromstring,
                               _commasplit(s[i+1:-1].strip())))
            return cls(rtype, atypes)
        if s == 'void' or s == 'none' or not s:  # void
            return cls()
        # atomic
        return cls(s)

    @classmethod
    def fromstring(cls, s):
        """Return new Type instance from a string.
        """
        try:
            return cls._fromstring(s)._normalize()
        except TypeParseError as msg:
            raise ValueError('failed to parse `%s`: %s' % (s, msg))

    @classmethod
    def fromnumba(cls, t):
        """Return new Type instance from numba type object.
        """
        if nb is None:
            raise RuntimeError('importing numba failed: %s' % (nb_NA_message))
        n = _numba_imap.get(t)
        if n is not None:
            return cls.fromstring(n)
        if isinstance(t, nb.typing.templates.Signature):
            atypes = map(cls.fromnumba, t.args)
            rtype = cls.fromnumba(t.return_type)
            return cls(rtype, tuple(atypes) or (Type(),))
        if isinstance(t, nb.types.misc.CPointer):
            return cls(cls.fromnumba(t.dtype), '*')
        raise NotImplementedError(repr(t))

    @classmethod
    def fromctypes(cls, t):
        """Return new Type instance from ctypes type object.
        """
        n = _ctypes_imap.get(t)
        if n is not None:
            return cls.fromstring(n)
        if issubclass(t, ctypes.Structure):
            return cls(*(cls.fromctypes(_t) for _f, _t in t._fields_))
        if issubclass(t, ctypes._Pointer):
            return cls(cls.fromctypes(t._type_), '*')
        if issubclass(t, ctypes._CFuncPtr):
            return cls(cls.fromctypes(t._restype_),
                       tuple(map(cls.fromctypes, t._argtypes_)) or (Type(),))
        raise NotImplementedError(repr(t))

    @classmethod
    def fromcallable(cls, func):
        """Return new Type instance from a callable object.

        The callable object must use annotations for specifying the
        types of arguments and return value.
        """
        if func.__name__ == '<lambda>':
            # lambda function cannot carry annotations, hence:
            raise ValueError('constructing Type instance from '
                             'a lambda function is not supported')
        sig = inspect.signature(func)
        annot = sig.return_annotation
        if annot == sig.empty:
            rtype = cls()  # void
            # TODO: check that function does not return other than None
        else:
            rtype = cls.fromobject(annot)
        atypes = []
        for n, param in sig.parameters.items():
            annot = param.annotation
            if param.kind not in [inspect.Parameter.POSITIONAL_OR_KEYWORD,
                                  inspect.Parameter.POSITIONAL_ONLY]:
                raise ValueError(
                    'callable argument kind must be positional,'
                    ' `%s` has kind %s' % (param, param.kind))
            if annot == sig.empty:
                atypes.append(cls('<type of %s>' % n))
            else:
                atypes.append(cls.fromobject(annot))
        return cls(rtype, tuple(atypes) or (Type(),))

    @classmethod
    def fromobject(cls, obj):
        """Return new Type instance from any object.
        """
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, str):
            return cls.fromstring(obj)
        n = _python_imap.get(obj)
        if n is not None:
            return cls.fromstring(n)
        if hasattr(obj, '__module__'):
            if obj.__module__.startswith('numba'):
                return cls.fromnumba(obj)
            if obj.__module__.startswith('ctypes'):
                return cls.fromctypes(obj)
        if inspect.isclass(obj):
            if obj is int:
                return cls('int64')
            return cls.fromstring(obj.__name__)
        if callable(obj):
            return cls.fromcallable(obj)
        raise NotImplementedError(repr(type(obj)))

    def _normalize(self):
        """Return new Type instance with atomic types normalized.
        """
        if self.is_void:
            return self
        if self.is_atomic:
            s = self[0]
            m = _int_match(s)
            if m is not None:
                bits = m.group(2)
                if not bits:
                    bits = '32'
                return self.__class__('int' + bits)
            m = _uint_match(s)
            if m is not None:
                bits = m.group(2)
                if not bits:
                    bits = '32'
                return self.__class__('uint' + bits)
            m = _float_match(s)
            if m is not None:
                bits = m.group(2)
                if not bits:
                    bits = '32'
                return self.__class__('float' + bits)
            m = _double_match(s)
            if m is not None:
                return self.__class__('float64')
            m = _complex_match(s)
            if m is not None:
                bits = m.group(2)
                if not bits:
                    bits = '64'
                return self.__class__('complex' + bits)
            m = _string_match(s)
            if m is not None:
                return self.__class__('string')
            m = _bool_match(s)
            if m is not None:
                return self.__class__('bool')
            m = _char_match(s)
            if m is not None:
                bits = m.group(2)
                if not bits:
                    bits = '8'
                return self.__class__('char' + bits)
            if s == 'byte':
                return self.__class__('int8')
            if s == 'ubyte':
                return self.__class__('uint8')
            if s == 'wchar':
                bits = str(ctypes.sizeof(ctypes.c_wchar) * 8)
                return self.__class__('char' + bits)
            return self
        if self.is_pointer:
            return self.__class__(self[0]._normalize(), self[1])
        if self.is_struct:
            return self.__class__(*(t._normalize() for t in self))
        if self.is_function:
            return self.__class__(self[0]._normalize(),
                                  tuple(t._normalize() for t in self[1]))
        raise NotImplementedError(repr(self))

    def mangle(self):
        """Return mangled type string.

        Mangled type string is a string representation of the type
        that can be used for extending the function name.
        """
        if self.is_void:
            return 'v'
        if self.is_pointer:
            return '_' + self[0].mangle() + 'P'
        if self.is_struct:
            return '_' + ''.join(m.mangle() for m in self) + 'K'
        if self.is_function:
            r = self[0].mangle()
            a = ''.join([a.mangle() for a in self[1]])
            return '_' + r + 'a' + a + 'A'
        if self.is_atomic:
            n = _mangling_map.get(self[0])
            if n is not None:
                return n
            n = self[0]
            return 'V' + str(len(n)) + 'V' + n
        raise NotImplementedError(repr(self))

    @classmethod
    def demangle(cls, s):
        block, rest = _demangle(s)
        assert not rest, repr(rest)
        assert len(block) == 1, repr(block)
        return block[0]


def _demangle(s):
    """Helper function to demangle the string of mangled Type.

    Used internally.

    Algorithm invented by Pearu Peterson, February 2019
    """
    if not s:
        return (Type(),), ''
    if s[0] == 'V':
        i = s.find('V', 1)
        assert i != -1, repr(s)
        ln = int(s[1:i])
        rest = s[i+ln+1:]
        typ = Type(s[i+1:i+ln+1])
    elif s[0] == '_':
        block, rest = _demangle(s[1:])
        kind, rest = rest[0], rest[1:]
        assert kind in '_'+_mangling_suffices+_mangling_prefixes, repr(kind)
        if kind == 'P':
            assert len(block) == 1, repr(block)
            typ = Type(block[0], '*')
        elif kind == 'K':
            typ = Type(*block)
        elif kind == 'a':
            assert len(block) == 1, repr(block)
            rtype = block[0]
            atypes, rest = _demangle('_' + rest)
            typ = Type(rtype, atypes)
        elif kind == 'A':
            return block, rest
        else:
            raise NotImplementedError(repr((kind, s)))
    else:
        rest = s[1:]
        t = _mangling_imap[s[0]]
        if t == 'void':
            typ = Type()
        else:
            typ = Type(t)
    result = [typ]
    if rest and rest[0] not in _mangling_prefixes:
        r, rest = _demangle(rest)
        result.extend(r)
    return tuple(result), rest
