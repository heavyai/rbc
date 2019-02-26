"""Provides Type structures.
"""
# Author: Pearu Peterson
# Created: February 2019


import re


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


_bool_match = re.compile(r'\A(bool|b)\Z').match
_string_match = re.compile(r'\A(string|str)\Z').match
_int_match = re.compile(r'\A(int|i)\s*(\d*)\Z').match
_uint_match = re.compile(r'\A(uint|u|unsigned\s*int)\s*(\d*)\Z').match
_float_match = re.compile(r'\A(float|f)\s*(16|32|64|128|256|)\Z').match
_double_match = re.compile(r'\A(double|d)\Z').match
_complex_match = re.compile(r'\A(complex|c)\s*(16|32|64|128|256|512|)\Z').match


class Type(tuple):
    """Represents a type.

    There are five kinds of a types:

      void        - a "no type"
      atomic      e.g. `int32`
      pointer     e.g. `int32`
      struct      e.g. `{int32, int32}`
      function    e.g. `int32(int32, int32)`

    Atomic types are types with names. All other types (except "no
    type") are certain constructions of other types.

    The name content of an atomic type is arbitrary but it cannot be
    empty. For instance, all the following Type instances represent
    atomic types: Type('a'), Type(('foo', )), Type(('foo', ))

    Parsing types from a string does not assume any particular type
    system. However, converting the Type instances to concrete types
    such as provided in numpy or numba, the following atomic types are
    defined (the first name corresponds to normalized name):

      no type:                    void
      bool:                       bool
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
        if not obj.is_ok:
            raise ValueError(
                'attempt to create an invalid type object from `%s`' % (args,))
        return obj

    @property
    def is_void(self):
        return len(self) == 0

    @property
    def is_atomic(self):
        return len(self) == 1 and isinstance(self[0], str)

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
    def is_ok(self):
        return self.is_void or self.is_atomic or self.is_pointer \
            or self.is_struct or self.is_function

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

    @classmethod
    def _fromstring(cls, s):
        """Parse type from a string.
        """
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
        if s == 'void' or not s:  # void
            return cls()
        # atomic
        return cls(s)

    @classmethod
    def fromstring(cls, s):
        try:
            return cls._fromstring(s)
        except TypeParseError as msg:
            raise ValueError('failed to parse `%s`: %s' % (s, msg))

    def normalize(self):
        """Return new type instance with atomic types normalized.
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
            if s == 'byte':
                return self.__class__('int8')
            if s == 'ubyte':
                return self.__class__('uint8')
            return self
        if self.is_pointer:
            return self.__class__(self[0].normalize(), self[1])
        if self.is_struct:
            return self.__class__(*(t.normalize() for t in self))
        if self.is_function:
            return self.__class__(self[0].normalize(),
                                  tuple(t.normalize() for t in self[1]))
        raise NotImplementedError(repr(self))
