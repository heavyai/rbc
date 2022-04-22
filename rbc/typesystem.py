"""Provides Type structures.
"""
# Author: Pearu Peterson
# Created: February 2019


import re
import copy
import ctypes
import _ctypes
import inspect
from llvmlite import ir
import warnings

from .targetinfo import TargetInfo
from .utils import check_returns_none
import numba as nb
import numpy as np
from numba.core import typing, datamodel, extending, typeconv
from numba.core.imputils import lower_cast


class TypeSystemManager:
    """Manages context specific aliases.

    Usage:

    .. code-block:: python

        with Type.alias(A='Array', bool='bool8', ...):
            # Type.fromstring('A a') will be replaced with Type.fromstring('Array a')

    """

    def alias(self, aliases):
        self.aliases = aliases
        return self

    def __enter__(self):
        self.old_aliases = Type.aliases.copy()
        new_aliases = self.old_aliases.copy()
        new_aliases.update(self.aliases)
        Type.aliases.clear()
        Type.aliases.update(new_aliases)

    def __exit__(self, exc_type, exc_value, traceback):
        Type.aliases.clear()
        Type.aliases.update(self.old_aliases)
        if exc_type is None:
            return True


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
    """Split a comma-separated items taking into account parenthesis
    and brackets.

    Used internally.
    """
    lst = s.split(',')
    ac = ''
    p1, p2, p3, p4 = 0, 0, 0, 0
    rlst = []
    for i in lst:
        p1 += i.count('(') - i.count(')')
        p2 += i.count('{') - i.count('}')
        p3 += i.count('[') - i.count(']')
        p4 += i.count('<') - i.count('>')
        if p1 == p2 == p3 == p4 == 0:
            rlst.append((ac + ',' + i if ac else i).strip())
            ac = ''
        else:
            ac = ac + ',' + i if ac else i
    if p1 == p2 == p3 == p4 == 0:
        return rlst
    raise TypeParseError('failed to comma-split `%s`' % s)


_booln_match = re.compile(r'\A(boolean|bool|b)(1|8)\Z').match
_charn_match = re.compile(r'\A(char)(32|16|8)(_t|)\Z').match
_intn_match = re.compile(r'\A(signed\s*int|int|i)(\d+)(_t|)\Z').match
_uintn_match = re.compile(r'\A(unsigned\s*int|uint|u)(\d+)(_t|)\Z').match
_floatn_match = re.compile(r'\A(float|f)(16|32|64|128|256)(_t|)\Z').match
_complexn_match = re.compile(
    r'\A(complex|c)(16|32|64|128|256|512)(_t|)\Z').match

_bool_match = re.compile(r'\A(boolean|bool|_Bool|b)\Z').match
_string_match = re.compile(r'\A(string|str)\Z').match

_char_match = re.compile(r'\A(char)\Z').match
_schar_match = re.compile(r'\A(signed\s*char)\Z').match
_uchar_match = re.compile(r'\A(unsigned\s*char|uchar)\Z').match
_byte_match = re.compile(r'\A(signed\s*byte|byte)\Z').match
_ubyte_match = re.compile(r'\A(unsigned\s*byte|ubyte)\Z').match
_wchar_match = re.compile(r'\A(wchar)(_t|)\Z').match

_short_match = re.compile(
    r'\A(signed\s*short\s*int|signed\s*short|short\s*int|short)\Z').match
_ushort_match = re.compile(
    r'\A(unsigned\s*short\s*int|unsigned\s*short|ushort)\Z').match

_int_match = re.compile(r'\A(signed\s*int|signed|int|i)\Z').match
_uint_match = re.compile(r'\A(unsigned\s*int|unsigned|uint|u)\Z').match

_long_match = re.compile(
    r'\A(signed\s*long\s*int|signed\s*long|long\s*int|long|l)\Z').match
_ulong_match = re.compile(
    r'\A(unsigned\s*long\s*int|unsigned\s*long|ulong)\Z').match

_longlong_match = re.compile(
    r'\A(signed\s*long\s*long\s*int|signed\s*long\s*long|long\s*long'
    r'\s*int|long\s*long)\Z').match
_ulonglong_match = re.compile(
    r'\A(unsigned\s*long\s*long\s*int|unsigned\s*long\s*long)\Z'
).match

_size_t_match = re.compile(r'\A(std::|c_|)size_t\Z').match
_ssize_t_match = re.compile(r'\A(std::|c_|)ssize_t\Z').match

_float_match = re.compile(r'\A(float|f)\Z').match
_double_match = re.compile(r'\A(double|d)\Z').match
_longdouble_match = re.compile(r'\A(long\s*double)\Z').match

_complex_match = re.compile(r'\A(complex|c)\Z').match


# For `Type.fromstring('<typespec> <name>')` support:
_type_name_match = re.compile(r'\A(.*)\s(\w+)\Z').match
# bad names are the names of types that can have modifiers such as
# `signed`, `unsigned`, `long`, etc.:
_bad_names_match = re.compile(r'\A(char|byte|short|int|long|double)\Z').match

# For the custom type support:
_custom_type_name_params_match = re.compile(r'\A(\w+)\s*[<](.*)[>]\Z').match


class Complex64(ctypes.Structure):
    _fields_ = [("real", ctypes.c_float), ("imag", ctypes.c_float)]

    @classmethod
    def from_param(cls, obj):
        if isinstance(obj, complex):
            return cls(obj.real, obj.imag)
        if isinstance(obj, (int, float)):
            return cls(obj.real, 0.0)
        raise NotImplementedError(repr(type(obj)))


class Complex128(ctypes.Structure):
    _fields_ = [("real", ctypes.c_double), ("imag", ctypes.c_double)]

    @classmethod
    def from_param(cls, obj):
        if isinstance(obj, complex):
            return cls(obj.real, obj.imag)
        if isinstance(obj, (int, float)):
            return cls(obj.real, 0.0)
        raise NotImplementedError(repr(type(obj)))

    def topython(self):
        return complex(self.real, self.imag)


# Initialize type maps
_ctypes_imap = {
    ctypes.c_void_p: 'void*', None: 'void', ctypes.c_bool: 'bool',
    ctypes.c_char_p: 'char%s*' % (ctypes.sizeof(ctypes.c_char()) * 8),
    ctypes.c_wchar_p: 'char%s*' % (ctypes.sizeof(ctypes.c_wchar()) * 8),
}
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
         ['c_float', 'c_double', 'c_longdouble']),
        ('complex', _ctypes_complex_map,
         [Complex64, Complex128])
]:
    for _n in _lst:
        if isinstance(_n, str):
            _t = getattr(ctypes, _n, None)
        else:
            _t = _n
        if _t is not None:
            _b = ctypes.sizeof(_t) * 8
            if _b not in _m:
                _m[_b] = _t
            _ctypes_imap[_t] = _k + str(_b)

_numba_imap = {nb.void: 'void', nb.boolean: 'bool'}
_numba_char_map = {}
_numba_bool_map = {}
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

# numpy mapping
_numpy_imap = {}
for v in set(np.typeDict.values()):
    name = np.dtype(v).name
    _numpy_imap[v] = name

# python_imap values must be processed with Type.fromstring
_python_imap = {int: 'int64', float: 'float64', complex: 'complex128',
                str: 'string', bytes: 'char*'}

# Data for the mangling algorithm, see mangle/demangle methods.
#
_mangling_suffices = '_VW'
_mangling_prefixes = 'PKaAMrR'
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


class MetaType(type):

    custom_types = dict()
    aliases = dict()

    # ctypes generated types need to be cached to be usable
    ctypes_types = dict()

    def __new__(cls, name, bases, dct):
        cls = super().__new__(cls, name, bases, dct)
        if name != 'Type':
            cls.custom_types[name] = cls
        return cls

    def alias(cls, **aliases):
        """
        Define type aliases context. For instance,

          with Type.alias(myint='int64'):
              ...

        will ensure under the with block we have

          Type.fromstring('myint') == Type.fromstring('int64')
        """
        return TypeSystemManager().alias(aliases)


class Type(tuple, metaclass=MetaType):
    """Represents a type.

    There are six kinds of a types:

      ==========    ==============================    ==============================
      Type          Description                       Internal structure
      ==========    ==============================    ==============================
      void          a "no type"                       Type()
      atomic        e.g. ``int32``                    Type(<str>,)
      pointer       e.g. ``int32*``                   Type(<Type instance>, '*')
      struct        e.g. ``{int32, int32}``           Type(<Type instances>, <Type instances>, ...)
      function      e.g. ``int32(int32, int32)``      Type(<Type instance>, ..., name='')
      custom        e.g. ``MyClass<int32, int32>``    Type((<object>,))
      undefined     e.g. ``fromcallable(foo)``        Type(None)
      ==========    ==============================    ==============================

    Atomic types are types with names (Type contains a single
    string). All other types (except "no type") are certain
    implementations of atomic types.

    The name content of an atomic type is arbitrary but it cannot be
    empty. For instance, Type('a') and Type('a long name') are atomic
    types.

    Parsing types from a string is not fixed to any type system, the
    names of types can be arbitrary.  However, converting the Type
    instances to concrete types such as provided in numpy or numba,
    the following atomic types are defined (the first name corresponds
    to normalized name)::

      no type:                    void, none
      bool:                       bool, boolean, _Bool, b
      1-bit bool:                 bool1, boolean1, b1
      8-bit bool:                 bool8, boolean8, b8
      8-bit char:                 char8, char
      16-bit char:                char16
      32-bit char:                char32, wchar
      8-bit signed integer:       int8, i8, byte, signed char
      16-bit signed integer:      int16, i16, int16_t
      32-bit signed integer:      int32, i32, int32_t
      64-bit signed integer:      int64, i64, int64_t
      128-bit signed integer:     int128, i128, int128_t
      8-bit unsigned integer:     uint8, u8, ubyte, unsigned char
      16-bit unsigned integer:    uint16, u16, uint16_t
      32-bit unsigned integer:    uint32, u32, uint32_t
      64-bit unsigned integer:    uint64, u64, uint64_t
      128-bit unsigned integer:   uint128, u128, uint64_t
      16-bit float:               float16, f16
      32-bit float:               float32, f32, float
      64-bit float:               float64, f64, double
      128-bit float:              float128, f128, long double
      32-bit complex:             complex32, c32, complex
      64-bit complex:             complex64, c64
      128-bit complex:            complex128, c128
      256-bit complex:            complex256, c256
      string:                     string, str

    with the following extensions::

      N-bit signed integer:   int<N>, i<N>       for instance: int5, i31
      N-bit unsigned integer: uint<N>, u<N>
      N-bit float:            float<N>
      N-bit complex:          complex<N>

    Also ``byte, short, int, long, long long, signed int, size_t,
    ssize_t``, etc are supported but their normalized names are system
    dependent.

    Custom types
    ------------

    The typesystem supports processing custom types that are usually
    named structures or C++ template specifications, but not only.

    Custom type can have arbitrary number of (hashable) parameters.  A
    custom type is concrete when the parameters with type Type are
    concrete.

    Custom types must implement tonumba method if needed. For that,
    one must derive MyClass from Type.

    Internally, a custom type has two possible representations: if
    MyClass is derived from Type then `MyClass<a, b>` is represented
    as `MyClass(('a', 'b'))`, otherwise, it is represented as
    `Type(('MyClass', 'a', 'b'))`. If the parameters of a custom types
    need to be Type instances, use `preprocess_args` for required
    conversion of arguments.
    """

    _mangling = None

    def __new__(cls, *args, **params):
        args = cls.preprocess_args(args)
        obj = tuple.__new__(cls, args)
        obj._params = params
        if not obj._is_ok:
            if obj._is_function:
                raise ValueError('cannot create named function type from `%s`' % (args,))
            raise ValueError(
                'attempt to create an invalid Type object from `%s`' % (args,))
        return obj.postprocess_type()

    def copy(self, cls=None):
        """Return a copy of type.
        """
        if cls is None:
            cls = type(self)
        return cls(*self, **copy.deepcopy(self._params))

    @classmethod
    def preprocess_args(cls, args):
        """Preprocess the arguments of Type constructor.

        Overloading this classmethod may be useful for custom types
        for processing its parameters.
        """
        return args

    def postprocess_type(self):
        """Postprocess Type construction. Must return Type instance.
        """
        return self

    def annotation(self, **annotations):
        """Set and get annotations.
        """
        annotation = self._params.get('annotation')
        if annotation is None:
            annotation = self._params['annotation'] = {}
        annotation.update(annotations)
        return annotation

    def __or__(self, other):
        """Apply annotations to a copy of type instance.
        """
        self.annotation()  # ensures annotation key in _params
        params = self._params.copy()
        annotation = params['annotation'] = params['annotation'].copy()
        if isinstance(other, str):
            if other:
                annotation[other] = ''
        elif isinstance(other, dict):
            annotation.update(other)
        return type(self)(*self, **params)

    def inherit_annotations(self, other):
        if isinstance(other, Type):
            self.annotation().update(other.annotation())
            for a, b in zip(self, other):
                if b is None:
                    continue
                if isinstance(a, str):
                    pass
                elif isinstance(a, Type):
                    a.inherit_annotations(b)
                elif isinstance(a, tuple):
                    for x, y in zip(a, b):
                        if isinstance(x, str):
                            pass
                        else:
                            x.inherit_annotations(y)
                else:
                    raise NotImplementedError('inherit_annotations: %s'
                                              % ((a, type(a)),))

    def params(self, other=None, **params):
        """In-place update of parameters from other or/and dictionary and return self.
        """
        if other is not None:
            return self.params(None, **other._params).params(None, **params)
        for k, v in params.items():
            if k == 'annotation':
                self.annotation().update(v)
            else:
                orig_v = self._params.get(k)
                if orig_v is not None and orig_v != v:
                    warnings.warn(
                        f'{type(self).__name__}.params: overwriting '
                        f'existing parameter {k} with {v} (original value is {orig_v})')
                self._params[k] = v
        return self

    def set_mangling(self, mangling):
        """Set mangling string of the type.
        """
        self._mangling = mangling

    def mangling(self):
        if self._mangling is None:
            self._mangling = self.mangle()
        return self._mangling

    @property
    def is_void(self):
        return len(self) == 0

    @property
    def is_atomic(self):
        return len(self) == 1 and isinstance(self[0], str)

    @property
    def is_undefined(self):
        return len(self) == 1 and self[0] is None

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
        return self.is_atomic and self[0].startswith('bool')

    @property
    def is_char(self):
        return self.is_atomic and self[0].startswith('char')

    @property
    def is_aggregate(self):
        # ref: https://llvm.org/docs/LangRef.html#aggregate-types
        return self.is_struct

    @property
    def is_pointer(self):
        return len(self) == 2 and isinstance(self[0], Type) \
            and isinstance(self[1], str) and self[1] == '*'

    @property
    def is_struct(self):
        return len(self) > 0 and all(isinstance(s, Type) for s in self)

    @property
    def _is_function(self):
        return len(self) == 2 and isinstance(self[0], Type) and \
            isinstance(self[1], tuple) and not isinstance(self[1], Type) \
            and all([isinstance(p, Type) for p in self[1]])

    @property
    def is_function(self):
        return self._is_function and 'name' in self._params

    @property
    def is_custom(self):
        return len(self) == 1 and not isinstance(self[0], Type) and isinstance(self[0], tuple)

    @property
    def is_signed(self):
        # https://en.cppreference.com/w/cpp/types/numeric_limits/is_signed
        return self.is_atomic and (self.is_char or self.is_int or self.is_float)

    @property
    def is_unsigned(self):
        # https://en.cppreference.com/w/cpp/types/numeric_limits/is_signed
        return self.is_atomic and not self.is_signed

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
                if not m.is_complete:
                    return False
        elif self.is_function:
            if not self[0].is_complete:
                return False
            for a in self[1]:
                if not a.is_complete:
                    return False
        elif self.is_void:
            pass
        elif self.is_custom:
            for a in self[0]:
                if isinstance(a, Type) and not a.is_complete:
                    return False
        elif self.is_undefined:
            return False
        else:
            raise NotImplementedError(repr(self))
        return True

    @property
    def is_concrete(self):
        """Return True when the Type instance is concrete.
        """
        if self.is_atomic:
            return (self.is_int or self.is_uint or self.is_float
                    or self.is_complex or self.is_bool
                    or self.is_string or self.is_char)
        elif self.is_pointer:
            return self[0].is_concrete
        elif self.is_struct:
            for m in self:
                if not m.is_concrete:
                    return False
        elif self.is_function:
            if not self[0].is_concrete:
                return False
            for a in self[1]:
                if not a.is_concrete:
                    return False
        elif self.is_void:
            pass
        elif self.is_custom:
            for a in self[0]:
                if isinstance(a, Type) and not a.is_concrete:
                    return False
        elif self.is_undefined:
            return False
        else:
            raise NotImplementedError(repr(self))
        return True

    @property
    def _is_ok(self):
        return self.is_void or self.is_atomic or self.is_pointer \
            or self.is_struct \
            or (self.is_function and len(self[1]) > 0) \
            or self.is_custom or self.is_undefined

    def __repr__(self):
        d = {}
        for k, v in self._params.items():
            if v:
                d[k] = v
        if d:
            return '%s%s|%s' % (type(self).__name__, tuple.__repr__(self), d)
        return '%s%s' % (type(self).__name__, tuple.__repr__(self))

    @property
    def consumes_nargs(self):
        """Return the number of arguments that the given type consumes.
        """
        return len(self.as_consumed_args)

    @property
    def as_consumed_args(self):
        """Return the argument types that the given type will consume.
        """
        return [self]

    @property
    def arity(self):
        """Return the arity of the function type.

        Some function argument types may consume several function
        arguments (e.g., data pointer and data size). The arity of
        function type is the number of functon arguments that are
        consumed when constructing a call.

        Arguments with default value are ignored.
        """
        assert self.is_function
        arity = 0
        for t in self[1]:
            if t.annotation().get('default'):
                break
            arity += t.consumes_nargs
        return arity

    @property
    def argument_types(self):
        """Return the list of consumed argument types.
        """
        assert self.is_function
        atypes = []
        for t in self[1]:
            atypes.extend(t.as_consumed_args)
        return atypes

    @property
    def name(self):
        """Return declarator name.

        Function types always define a name: when not specified then
        the name value will be an empty string.

        For other types the name is optional: when not specified then
        the name value will be None.
        """
        return self._params.get('name')

    def get_field_position(self, name):
        """Return the index of a structure member with name.

        Returns None when no match with member names is found.
        """
        assert self.is_struct
        for i, m in enumerate(self):
            if m.name == name:
                return i

    def __str__(self):
        if self._is_ok:
            return self.tostring()
        return tuple.__str__(self)

    def tostring(self, use_typename=False, use_annotation=True, use_name=True,
                 use_annotation_name=False, _skip_annotation=False):
        """Return string representation of a type.
        """
        options = dict(use_typename=use_typename, use_annotation=use_annotation,
                       use_name=use_name,
                       use_annotation_name=use_annotation_name)
        annotation = self.annotation()
        if use_annotation and not _skip_annotation:
            s = self.tostring(_skip_annotation=True, **options)
            for name, value in annotation.items():
                if (
                        use_annotation_name
                        and use_name
                        and name == 'name'
                        and self._params.get('name', value) == value):
                    continue
                if value:
                    s = '%s | %s=%s' % (s, name, value)
                else:
                    s = '%s | %s' % (s, name)
            return s

        if self.is_void:
            return 'void'

        name = self._params.get('name') if use_name else None
        if use_annotation_name and use_name:
            annot_name = annotation.get('name')
            if annot_name is not None and name is None:
                name = annot_name

        if self.is_function:
            if use_typename:
                typename = self._params.get('typename')
                if typename is not None:
                    return typename
            name = ' ' + name if name else ''
            return (self[0].tostring(**options)
                    + name + '(' + ', '.join(a.tostring(**options) for a in self[1]) + ')')

        if name is not None:
            suffix = ' ' + name
        else:
            suffix = ''
        if use_typename:
            typename = self._params.get('typename')
            if typename is not None:
                return typename + suffix
        if self.is_atomic:
            return self[0] + suffix
        if self.is_pointer:
            return self[0].tostring(**options) + '*' + suffix
        if self.is_struct:
            clsname = self._params.get('clsname')
            if clsname is not None:
                return clsname + '<' + ', '.join(
                    [t.tostring(**options)
                     for t in self]) + '>' + suffix
            return '{' + ', '.join([t.tostring(**options) for t in self]) + '}' + suffix

        if self.is_custom:
            params = self[0]
            if type(self) is Type:
                name = params[0]
                params = params[1:]
            else:
                name = type(self).__name__
            name = self._params.get('shorttypename', name)
            new_params = []
            for a in params:
                if isinstance(a, Type):
                    s = a.tostring(**options)
                else:
                    s = str(a)
                new_params.append(s)
            return (name + '<' + ', '.join(new_params) + '>' + suffix)
        raise NotImplementedError(repr(self))

    def toprototype(self):
        if self.is_void:
            return 'void'
        typename = self._params.get('typename')
        if typename is not None:
            return typename
        if self.is_atomic:
            s = self[0]
            if self.is_int or self.is_uint:
                return s + '_t'
            if self.is_float:
                bits = self.bits
                if bits == 32:
                    return 'float'
                elif bits == 64:
                    return 'double'
            return s
        if self.is_pointer:
            return self[0].toprototype() + '*'
        if self.is_struct:
            return '{' + ', '.join([t.toprototype() for t in self]) + '}'
        if self.is_function:
            return self[0].toprototype() + '(' + ', '.join(
                a.toprototype() for a in self[1]) + ')'
        if self.is_custom:
            raise NotImplementedError(f'{type(self).__name__}.toprototype()')
        raise NotImplementedError(repr(self))

    def tonumba(self, bool_is_int8=None):
        """Convert Type instance to numba type object.

        Parameters
        ----------
        bool_is_int8: {bool, None}

          If true, boolean data and values are mapped to LLVM `i8`,
          otherwise to `i1`. Note that numba boolean maps data to `i8`
          and value to `i1`. To get numba convention, specify
          `bool_is_int8` as `None`.

        """
        numba_type = self._params.get('tonumba')
        if numba_type is not None:
            return numba_type
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
            if bool_is_int8 is None:
                return _numba_bool_map.get(self.bits, nb.boolean)
            return boolean8 if bool_is_int8 else boolean1
        if self.is_pointer:
            if self[0].is_void:
                return nb.types.voidptr
            if self[0].is_struct:
                ptr_type = self._params.get('NumbaType', structure_type.StructureNumbaPointerType)
            else:
                ptr_type = self._params.get('NumbaType', nb.types.CPointer)
            return ptr_type(self[0].tonumba(bool_is_int8=bool_is_int8))
        if self.is_struct:
            struct_name = self._params.get('name')
            if struct_name is None:
                struct_name = 'STRUCT'+self.mangling()
            members = []
            for i, member in enumerate(self):
                name = member._params.get('name', '_%s' % (i+1))
                members.append((name,
                                member.tonumba(bool_is_int8=bool_is_int8)))
            return structure_type.make_numba_struct(struct_name, members, origin=self)
        if self.is_function:
            rtype = self[0].tonumba(bool_is_int8=bool_is_int8)
            atypes = [t.tonumba(bool_is_int8=bool_is_int8)
                      for t in self[1] if not t.is_void]
            return rtype(*atypes)
        if self.is_string:
            return nb.types.string
        if self.is_char:
            # in numba, char==int8
            return _numba_int_map.get(int(self[0][4:]))
        if self.is_atomic:
            return nb.types.Type(self[0])
        if self.is_custom:
            return self.__typesystem_type__.tonumba(bool_is_int8=bool_is_int8)
        raise NotImplementedError(repr(self))

    def toctypes(self):
        """Convert Type instance to ctypes type object.
        """
        if self.is_void:
            return None
        if self.is_int:
            return _ctypes_int_map[int(self[0][3:])]
        if self.is_uint:
            return _ctypes_uint_map[int(self[0][4:])]
        if self.is_float:
            return _ctypes_float_map[int(self[0][5:])]
        if self.is_complex:
            return _ctypes_complex_map[int(self[0][7:])]
        if self.is_bool:
            return ctypes.c_bool
        if self.is_char:
            return _ctypes_char_map[int(self[0][4:])]
        if self.is_pointer:
            if self[0].is_void:
                return ctypes.c_void_p
            if self[0].is_char:
                return getattr(ctypes,
                               _ctypes_char_map.get(
                                   int(self[0][0][4:])).__name__ + '_p')
            return ctypes.POINTER(self[0].toctypes())
        if self.is_struct:
            ctypes_type_name = 'rbc_typesystem_struct_%s' % (self.mangle())
            typ = type(self).ctypes_types.get(ctypes_type_name)
            if typ is None:
                fields = [((t.name if t.name else 'f%s' % i), t.toctypes())
                          for i, t in enumerate(self)]
                typ = type(ctypes_type_name,
                           (ctypes.Structure, ),
                           dict(_fields_=fields))
                type(self).ctypes_types[ctypes_type_name] = typ
            return typ
        if self.is_function:
            ctypes_type_name = 'rbc_typesystem_function_%s' % (self.mangle())
            typ = type(self).ctypes_types.get(ctypes_type_name)
            if typ is None:
                rtype = self[0].toctypes()
                atypes = []
                for t in self[1]:
                    if t.is_struct:
                        # LLVM struct argument (as an aggregate type)
                        # is mapped to struct member arguments:
                        atypes.extend([m.toctypes() for m in t])
                    elif t.is_void:
                        pass
                    else:
                        atypes.append(t.toctypes())
                typ = ctypes.CFUNCTYPE(rtype, *atypes)
                type(self).ctypes_types[ctypes_type_name] = typ
            return typ
        if self.is_string:
            return ctypes.c_wchar_p
        if self.is_custom:
            raise NotImplementedError(f'{type(self).__name__}.toctypes()|self={self}')
        raise NotImplementedError(repr((self, self.is_string)))

    def tollvmir(self, bool_is_int8=None):
        if self.is_int:
            return ir.IntType(self.bits)
        if self.is_float:
            return {32: ir.FloatType, 64: ir.DoubleType}[self.bits]()
        if self.is_bool:
            if bool_is_int8:
                return ir.IntType(8)
            return ir.IntType(self.bits)
        if self.is_pointer:
            if self[0].is_void:
                # mapping void* to int8*
                return ir.IntType(8).as_pointer()
            return self[0].tollvmir(bool_is_int8=bool_is_int8).as_pointer()
        if self.is_void:
            # Used only as the return type of a function without a return value.
            # TODO: ensure that void is used only as function return type or a pointer dtype.
            return ir.VoidType()
        if self.is_struct:
            return ir.LiteralStructType([m.tollvmir(bool_is_int8=bool_is_int8) for m in self])
        raise NotImplementedError(f'{type(self).__name__}.tollvmir()|self={self}')

    @classmethod
    def _fromstring(cls, s):
        s = s.strip()
        if len(s) > 1 and s.endswith('*'):       # pointer
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
            atypes = tuple(map(cls._fromstring,
                               _commasplit(s[i+1:-1].strip())))
            r = s[:i].strip()
            if r.endswith(')'):
                j = _findparen(r)
                if j < 0:
                    raise TypeParseError('mismatching parenthesis in `%s`' % (r))
                rtype = cls._fromstring(r[:j])
                d = r[j+1:-1].strip()
                if d.startswith('*'):
                    while d.startswith('*'):
                        d = d[1:].lstrip()
                    if d.endswith(')'):
                        k = _findparen(d)
                        name = d[:k]
                        rtype = cls(rtype, atypes, name='')
                        atypes = tuple(map(cls._fromstring,
                                           _commasplit(d[k+1:-1].strip())))
                        return cls(rtype, atypes, name=name)
                    name = d
                    return cls(rtype, atypes, name=name)
            rtype = cls._fromstring(r)
            if rtype.is_function:
                name = rtype._params['name']
                rtype._params['name'] = ''
            else:
                name = rtype._params.pop('name', '')
            return cls(rtype, atypes, name=name)

        i_bar, i_gt = s.find('|'), s.find('>')  # TODO: will need a better parser
        if '|' in s and (i_gt == -1 or i_bar > i_gt):
            s, a = s.rsplit('|', 1)
            t = cls._fromstring(s.rstrip())
            if '=' in a:
                n, v = a.split('=', 1)
            else:
                n, v = a, ''
            n = n.strip()
            v = v.strip()
            if n or v:
                t.annotation(**{n: v})
            return t
        if s.endswith('>') and not s.startswith('<'):  # custom
            i = s.index('<')
            name = s[:i]
            params = _commasplit(s[i+1:-1].strip())
            params = tuple(map(cls._fromstring, params)) if params else ()
            name = cls.aliases.get(name, name)
            if name in cls.custom_types:
                cls = cls.custom_types[name]
                r = cls(params)
            else:
                r = cls((name,) + params)
            return r
        if s == 'void' or s == 'none' or not s:  # void
            return cls()
        m = _type_name_match(s)
        if m is not None:
            name = m.group(2)
            if not _bad_names_match(name):
                # `<typespec> <name>`
                t = cls._fromstring(m.group(1))
                t._params['name'] = name
                return t
        # atomic
        if s in cls.custom_types:
            return cls.custom_types[s](())
        return cls(s)

    @classmethod
    def fromstring(cls, s):
        """Return new Type instance from a string.

        Parameters
        ----------
        s : str
        """
        try:
            return cls._fromstring(s)._normalize()
        except TypeParseError as msg:
            raise ValueError('failed to parse `%s`: %s' % (s, msg))

    @classmethod
    def fromnumpy(cls, t):
        """Return new Type instance from numpy type object.
        """
        n = _numpy_imap.get(t)
        if n is not None:
            return cls.fromstring(n)

        raise NotImplementedError(repr(t))

    @classmethod
    def fromnumba(cls, t):
        """Return new Type instance from numba type object.
        """
        if hasattr(t, "__typesystem_type__") or hasattr(type(t), "__typesystem_type__"):
            return t.__typesystem_type__
        n = _numba_imap.get(t)
        if n is not None:
            return cls.fromstring(n)
        if isinstance(t, typing.templates.Signature):
            atypes = (cls.fromnumba(a) for a in t.args)
            rtype = cls.fromnumba(t.return_type)
            return cls(rtype, tuple(atypes) or (Type(),), name='')
        if isinstance(t, nb.types.misc.CPointer):
            return cls(cls.fromnumba(t.dtype), '*')
        if isinstance(t, nb.types.NumberClass):
            return cls.fromnumba(t.instance_type)
        if isinstance(t, nb.types.Boolean):
            # boolean1 and boolean8 map both to bool
            return cls.fromstring('bool')
        if isinstance(t, nb.types.misc.RawPointer):
            if t == nb.types.voidptr:
                return cls(cls(), '*')

        raise NotImplementedError(repr((t, type(t).__bases__)))

    @classmethod
    def fromctypes(cls, t):
        """Return new Type instance from ctypes type object.
        """
        n = _ctypes_imap.get(t)
        if n is not None:
            return cls.fromstring(n)
        if issubclass(t, ctypes.Structure):
            return cls(*(cls.fromctypes(_t)
                         for _f, _t in t._fields_))
        if issubclass(t, ctypes._Pointer):
            return cls(cls.fromctypes(t._type_), '*')
        if issubclass(t, ctypes._CFuncPtr):
            atypes = tuple(cls.fromctypes(a) for a in t._argtypes_)
            return cls(cls.fromctypes(t._restype_), atypes or (Type(),), name='')
        raise NotImplementedError(repr(t))

    @classmethod
    def fromcallable(cls, func):
        """Return new Type instance from a callable object.

        The callable object must use annotations for specifying the
        types of arguments and return value.
        """
        if not hasattr(func, '__name__'):
            raise ValueError(
                'constructing Type instance from a callable without `__name__`'
                f' is not supported, got {func}|{type(func).__bases__}')
        if func.__name__ == '<lambda>':
            # lambda function cannot carry annotations, hence:
            raise ValueError('constructing Type instance from '
                             'a lambda function is not supported')
        sig = get_signature(func)
        annot = sig.return_annotation
        if isinstance(annot, dict):
            rtype = cls() | annot  # void
        elif annot == sig.empty:
            if check_returns_none(func):
                rtype = cls()  # void
            else:
                rtype = cls(None)   # cannot deterimine return value type
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
            if param.default != sig.empty:
                if annot == sig.empty:
                    annot = {}
                annot['default'] = param.default
            if isinstance(annot, dict):
                atypes.append(cls('<type of %s>' % n) | annot)
            elif annot == sig.empty:
                atypes.append(cls('<type of %s>' % n))
            else:
                atypes.append(cls.fromobject(annot))

        return cls(rtype, tuple(atypes) or (Type(),), name=func.__name__)

    @classmethod
    def fromvalue(cls, obj):
        """Return Type instance that corresponds to given value.
        """
        for mapping in [_python_imap, _numpy_imap]:
            n = mapping.get(type(obj))
            if n is not None:
                return cls.fromstring(n)
        if isinstance(obj, _ctypes._Pointer):
            return cls.fromctypes(obj._type_).pointer()
        if isinstance(obj, ctypes.c_void_p):
            return cls(cls(), '*')
        if hasattr(obj, '__typesystem_type__'):
            return cls.fromobject(obj.__typesystem_type__)

        raise NotImplementedError('%s.fromvalue(%r|%s)'
                                  % (cls.__name__, obj, type(obj)))

    @classmethod
    def fromobject(cls, obj):
        """Return new Type instance from any object.

        Parameters
        ----------
        obj : object
        """
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, str):
            return cls.fromstring(obj)
        n = _python_imap.get(obj)
        if n is not None:
            return cls.fromstring(n)
        if hasattr(obj, "__typesystem_type__") or hasattr(type(obj), "__typesystem_type__"):
            return obj.__typesystem_type__
        if hasattr(obj, '__module__'):
            if obj.__module__.startswith('numba'):
                return cls.fromnumba(obj)
            if obj.__module__.startswith('ctypes'):
                return cls.fromctypes(obj)
            if obj.__module__.startswith('numpy'):
                return cls.fromnumpy(obj)
        if inspect.isclass(obj):
            if obj is int:
                return cls('int64')
            return cls.fromstring(obj.__name__)
        if callable(obj):
            return cls.fromcallable(obj)
        raise NotImplementedError(repr((type(obj))))

    def _normalize(self):
        """Return new Type instance with atomic types normalized.
        """
        params = self._params
        if self.is_void:
            return self
        if self.is_atomic:
            s = self[0]
            a = type(self).aliases.get(s)
            if a is not None:
                return Type.fromobject(a).params(self)._normalize()
            m = _string_match(s)
            if m is not None:
                return Type('string', **params)
            for match, ntype in [
                    (_booln_match, 'bool'),
                    (_charn_match, 'char'),
                    (_intn_match, 'int'),
                    (_uintn_match, 'uint'),
                    (_floatn_match, 'float'),
                    (_complexn_match, 'complex'),
            ]:
                m = match(s)
                if m is not None:
                    bits = m.group(2)
                    return Type(ntype + bits, **params)
            if s.endswith('[]'):
                return Type.fromstring(f'Array<{s[:-2]}>')._normalize()
            m = _bool_match(s)
            if m is not None:
                return Type('bool', **params)
            if _char_match(s):
                return Type('char8', **params)    # IEC 9899 defines sizeof(char)==1
            if _float_match(s):
                return Type('float32', **params)  # IEC 60559 defines sizeof(float)==4
            if _double_match(s):
                return Type('float64', **params)  # IEC 60559 defines sizeof(double)==8
            target_info = TargetInfo()
            for match, otype, ntype in [
                    (_wchar_match, 'wchar', 'char'),

                    (_schar_match, 'char', 'int'),
                    (_uchar_match, 'uchar', 'uint'),

                    (_byte_match, 'byte', 'int'),
                    (_ubyte_match, 'ubyte', 'uint'),

                    (_short_match, 'short', 'int'),
                    (_ushort_match, 'ushort', 'uint'),

                    (_int_match, 'int', 'int'),
                    (_uint_match, 'uint', 'uint'),

                    (_long_match, 'long', 'int'),
                    (_ulong_match, 'ulong', 'uint'),

                    (_longlong_match, 'longlong', 'int'),
                    (_ulonglong_match, 'ulonglong', 'uint'),

                    (_size_t_match, 'size_t', 'uint'),
                    (_ssize_t_match, 'ssize_t', 'int'),

                    (_longdouble_match, 'longdouble', 'float'),
                    (_complex_match, 'complex', 'complex'),
            ]:
                if match(s) is not None:
                    sz = target_info.sizeof(otype)
                    if sz is not None:
                        bits = str(sz * 8)
                        return Type(ntype + bits, **params)
                    break
            if target_info.strict:
                raise ValueError('%s is not concrete' % (self))
            return self
        if self.is_pointer:
            return Type(
                self[0]._normalize(), self[1], **params)
        if self.is_struct:
            return Type(
                *(t._normalize() for t in self), **params)
        if self.is_function:
            return Type(
                self[0]._normalize(),
                tuple(t._normalize() for t in self[1]),
                **params)
        if self.is_custom:
            inner = []
            for a in self[0]:
                if isinstance(a, Type):
                    a = a._normalize()
                inner.append(a)
            return type(self)(tuple(inner), **params)
        raise NotImplementedError(repr(self))

    def mangle(self):
        """Return mangled type string.

        Mangled type string is a string representation of the type
        that can be used for extending the function name.
        """
        name_suffix = ''
        name = self.name
        if name:
            name_suffix = 'W' + str(len(name)) + 'W' + name
        if self.is_void:
            assert name_suffix == '', name_suffix
            return 'v'
        if self.is_pointer:
            return '_' + self[0].mangle() + 'P' + name_suffix
        if self.is_struct:
            return '_' + ''.join(m.mangle()
                                 for m in self) + 'K' + name_suffix
        if self.is_function:
            r = self[0].mangle()
            a = ''.join([a.mangle() for a in self[1]])
            return '_' + r + 'a' + a + 'A' + name_suffix
        if self.is_custom:
            params = self[0]
            if type(self) is Type:
                name = params[0]
                params = params[1:]
            else:
                name = type(self).__name__
            n = _mangling_map.get(name)
            n = name if n is None else n
            r = 'V' + str(len(n)) + 'V' + n
            a = ''.join([a.mangle() for a in params])
            return '_' + r + 'r' + a + 'R' + name_suffix
        if self.is_atomic:
            n = _mangling_map.get(self[0])
            if n is not None:
                return n + name_suffix
            n = self[0]
            return 'V' + str(len(n)) + 'V' + n + name_suffix
        raise NotImplementedError(repr(self))

    @classmethod
    def demangle(cls, s):
        block, rest = _demangle(s)
        assert not rest, repr(rest)
        assert len(block) == 1, repr(block)
        return block[0]

    @property
    def bits(self):
        if self.is_void:
            return 0
        if self.is_bool:
            return int(self[0][4:] or 1)
        if self.is_int:
            return int(self[0][3:])
        if self.is_uint or self.is_char:
            return int(self[0][4:])
        if self.is_float:
            return int(self[0][5:])
        if self.is_complex:
            return int(self[0][7:])
        if self.is_struct:
            return sum([m.bits for m in self])
        if self.is_pointer:
            return TargetInfo().sizeof('voidptr')*8 or 64
        return NotImplemented

    def match(self, other):
        """Return match penalty when other can be converted to self.
        Otherwise, return None.

        Parameters
        ----------
        other : {Type, tuple}
          Specify other signature. If other is a tuple of signatures,
          then it is interpreted as argument types of a function
          signature.

        Returns
        -------
        penalty : {int, None}
          Penalty of a match. For a perfect match, penalty is 0.
          If match is impossible, return None
        """
        if isinstance(other, Type):
            if self == other:
                return 0
            if other.is_void:
                return (0 if self.is_void else None)
            elif self.is_custom:
                if type(self) is not type(other):
                    return
                if self[0] == other[0]:
                    if self._params or other._params:
                        warnings.warn(
                            'The default match implementation ignores _params content. Please'
                            f' implement match method for custom type {type(self).__name__}')
                    return 1
                return
            elif other.is_pointer:
                if not self.is_pointer:
                    return
                if self[0].is_void or other[0].is_void:
                    return 0
                penalty = self[0].match(other)
                if penalty is None:
                    if self[0].is_void:
                        penalty = 1
                return penalty
            elif self.is_pointer:
                return
            elif other.is_struct:
                if not self.is_struct:
                    return
                if len(self) != len(other):
                    return
                penalty = 0
                for a, b in zip(self, other):
                    p = a.match(b)
                    if p is None:
                        return
                    penalty = penalty + p
                return penalty
            elif self.is_struct:
                return
            elif other.is_function:
                if not self.is_function:
                    return
                if len(self[1]) != len(other[1]):
                    return
                penalty = self[0].match(other[0])
                if penalty is None:
                    return
                for a, b in zip(self[1], other[1]):
                    p = a.match(b)
                    if p is None:
                        return
                    penalty = penalty + p
                return penalty
            elif self.is_function:
                return
            elif (
                    (other.is_int and self.is_int)
                    or (other.is_bool and self.is_bool)
                    or (other.is_float and self.is_float)
                    or (other.is_uint and self.is_uint)
                    or (other.is_char and self.is_char)
                    or (other.is_complex and self.is_complex)):
                if self.bits >= other.bits:
                    return 0
                return other.bits - self.bits
            elif (
                    (other.is_int and self.is_uint)
                    or (other.is_uint and self.is_int)):
                if self.bits >= other.bits:
                    return 10
                return 10 + other.bits - self.bits
            elif self.is_complex and (other.is_float
                                      or other.is_int or other.is_uint):
                return 1000
            elif self.is_float and (other.is_int or other.is_uint):
                return 1000
            elif (self.is_float or self.is_complex) and other.is_bool:
                return
            elif (self.is_int or self.is_uint) and other.is_bool:
                if self.bits >= other.bits:
                    return 0
                return other.bits - self.bits
            elif self.is_bool and (other.is_int or other.is_uint):
                if self.bits >= other.bits:
                    return 0
                return other.bits - self.bits
            elif ((self.is_int or self.is_uint or self.is_bool)
                  and (other.is_float or other.is_complex)):
                return
            elif self.is_float and other.is_complex:
                return
            elif self.is_pointer and (other.is_int or other.is_uint):
                if self.bits == other.bits:
                    return 1
                return
            elif ((self.is_string and not other.is_string)
                  or (not self.is_string and other.is_string)):
                return

            # TODO: lots of
            raise NotImplementedError(repr((self, other)))
        elif isinstance(other, tuple):
            if not self.is_function:
                return
            atypes = self[1]
            if len(other) == 0 and len(atypes) == 1 and atypes[0].is_void:
                return 0
            if len(atypes) != len(other):
                return
            penalty = 0
            for a, b in zip(atypes, other):
                p = a.match(b)
                if p is None:
                    return
                penalty = penalty + p
            return penalty
        raise NotImplementedError(repr(type(other)))

    def __call__(self, *atypes, **params):
        return Type(self, atypes or (Type(),), **params)

    def pointer(self):
        numba_ptr_type = self._params.get('NumbaPointerType')
        if numba_ptr_type is not None:
            return Type(self, '*').params(NumbaType=numba_ptr_type)
        return Type(self, '*')

    def apply_templates(self, templates):
        """Iterator of concrete types derived from applying templates mapping
        to self.

        The caller must allow templates dictionary to be changed in-situ.
        """
        if self.is_concrete:
            yield self
        elif self.is_atomic:
            type_list = templates.get(self[0])
            if type_list:
                assert isinstance(type_list, list), type_list
                for i, t in enumerate(type_list):
                    # using copy so that template symbols will not be
                    # contaminated with self parameters
                    t = Type.fromobject(t)
                    t_ = t.copy()
                    if t.is_concrete:
                        # templates is changed in-situ! This ensures that
                        # `T(T)` produces `i4(i4)`, `i8(i8)` for `T in
                        # [i4, i8]`. To produce all possible combinations
                        # `i4(i4)`, `i8(i8)`, `i8(i4)`, `i4(i8)`, use
                        # `T1(T2)` where `T1 in [i4, i8]` and `T2 in [i4,
                        # i8]`
                        templates[self[0]] = [t]
                        yield t_.params(None, **self._params)
                        # restore templates
                        templates[self[0]] = type_list
                    else:
                        # this will avoid infinite recursion when
                        # templates is `T in [U]` and `U in [T]`
                        templates[self[0]] = []
                        for ct in t.apply_templates(templates):
                            templates[self[0]] = [ct]
                            yield ct.params(None, **self._params)
                            templates[self[0]] = []
                        templates[self[0]] = type_list
            else:
                raise TypeError(f'cannot make {self} concrete using template mapping {templates}')
        elif self.is_pointer:
            for t in self[0].apply_templates(templates):
                yield Type(t, self[1], **self._params)
        elif self.is_struct:
            for i, t in enumerate(self):
                if not t.is_concrete:
                    for ct in t.apply_templates(templates):
                        yield from Type(
                            *(self[:i] + (ct,) + self[i+1:]),
                            **self._params).apply_templates(templates)
                    return
            assert 0
        elif self.is_function:
            rtype, atypes = self
            if not rtype.is_concrete:
                for rt in rtype.apply_templates(templates):
                    yield from Type(rt, atypes, **self._params).apply_templates(templates)
                return
            for i, t in enumerate(atypes):
                if not t.is_concrete:
                    for ct in t.apply_templates(templates):
                        yield from Type(
                            rtype, atypes[:i] + (ct,) + atypes[i+1:],
                            **self._params).apply_templates(templates)
                    return
            assert 0
        elif self.is_custom:
            params = self[0]
            cls = type(self)
            if cls is Type:
                name = params[0]
                params = params[1:]
            else:
                name = cls.__name__
            if name in templates:
                for cname in templates[name]:
                    cname = Type.aliases.get(cname, cname)
                    if isinstance(cname, str):
                        cls = Type.custom_types.get(cname, Type)
                    else:
                        assert issubclass(cname, Type), cname
                        cls = cname
                        cname = cls.__name__
                    if cls is Type:
                        typ = cls((cname,) + params)
                    else:
                        typ = cls(params)
                    yield from typ.params(self).apply_templates(templates)
                return
            for i, t in enumerate(params):
                if not isinstance(t, Type):
                    continue
                if not t.is_concrete:
                    for ct in t.apply_templates(templates):
                        if cls is Type:
                            yield from cls(
                                (name,) + params[:i] + (ct,) + params[i+1:],
                                **self._params).apply_templates(templates)
                        else:
                            yield from cls(
                                params[:i] + (ct,) + params[i+1:],
                                **self._params).apply_templates(templates)
                    return
            assert 0, repr(self)
        else:
            raise NotImplementedError(f'apply_templates: {self} {templates}')


def _demangle_name(s, suffix='W'):
    if s and s[0] == suffix:
        i = s.find(suffix, 1)
        assert i != -1, repr(s)
        ln = int(s[1:i])
        rest = s[i+ln+1:]
        return s[i+1:i+ln+1], rest
    return None, s


def _demangle(s):
    """Helper function to demangle the string of mangled Type.

    Used internally.

    Algorithm invented by Pearu Peterson, February 2019
    """
    if not s:
        return (Type(),), ''
    if s[0] == 'V':
        name, rest = _demangle_name(s, suffix='V')
        typ = Type(name)
    elif s[0] == '_':
        block, rest = _demangle(s[1:])
        kind, rest = rest[0], rest[1:]
        assert kind in '_'+_mangling_suffices+_mangling_prefixes, repr(kind)
        if kind == 'P':
            assert len(block) == 1, repr(block)
            typ = Type(block[0], '*')
        elif kind == 'K':
            typ = Type(*block)
        elif kind == 'a':  # function
            assert len(block) == 1, repr(block)
            rtype = block[0]
            atypes, rest = _demangle('_' + rest)
            typ = Type(rtype, atypes, name='')
        elif kind in 'AR':
            return block, rest
        elif kind == 'r':  # custom
            assert len(block) == 1, repr(block)
            name = block[0]
            assert name.is_atomic, name
            name = name[0]
            if rest and rest[0] == 'R':
                atypes = ()
                rest = rest[1:]
            else:
                atypes, rest = _demangle('_' + rest)
            if name in Type.custom_types:
                typ = Type.custom_types[name](atypes)
            else:
                typ = Type((name,)+atypes)
        else:
            raise NotImplementedError(repr((kind, s)))
    else:
        rest = s[1:]
        t = _mangling_imap[s[0]]
        if t == 'void':
            typ = Type()
        else:
            typ = Type(t)
    vname, rest = _demangle_name(rest)
    if vname is not None:
        typ._params['name'] = vname
    result = [typ]
    if rest and rest[0] not in _mangling_prefixes:
        r, rest = _demangle(rest)
        result.extend(r)
    return tuple(result), rest


# TODO: move numba boolean support to rbc/boolean_type.py

class Boolean1(nb.types.Boolean):

    def can_convert_from(self, typingctx, other):
        return isinstance(other, nb.types.Boolean)


@datamodel.register_default(Boolean1)
class Boolean1Model(datamodel.models.BooleanModel):

    def get_data_type(self):
        return self._bit_type


class Boolean8(nb.types.Boolean):

    bitwidth = 8

    def can_convert_to(self, typingctx, other):
        return isinstance(other, nb.types.Boolean)

    def can_convert_from(self, typingctx, other):
        return isinstance(other, nb.types.Boolean)


@datamodel.register_default(Boolean8)
class Boolean8Model(datamodel.models.BooleanModel):

    def get_value_type(self):
        return self._byte_type


boolean1 = Boolean1('boolean1')
boolean8 = Boolean8('boolean8')


@lower_cast(Boolean1, nb.types.Boolean)
@lower_cast(Boolean8, nb.types.Boolean)
def booleanN_to_boolean(context, builder, fromty, toty, val):
    return builder.icmp_signed('!=', val, val.type(0))


@lower_cast(nb.types.BooleanLiteral, Boolean1)
@lower_cast(nb.types.BooleanLiteral, Boolean8)
@lower_cast(nb.types.Boolean, Boolean1)
@lower_cast(nb.types.Boolean, Boolean8)
def boolean_to_booleanN(context, builder, fromty, toty, val):
    llty = context.get_value_type(toty)
    return builder.zext(val, llty)


@extending.lower_builtin(bool, Boolean8)
def boolean8_to_bool(context, builder, sig, args):
    [val] = args
    return builder.icmp_signed('!=', val, val.type(0))


_numba_bool_map[1] = boolean1
_numba_bool_map[8] = boolean8
_numba_imap[boolean1] = 'bool1'
_numba_imap[boolean8] = 'bool8'

boolean8ptr = nb.types.CPointer(boolean8)
boolean8ptr2 = nb.types.CPointer(boolean8ptr)

_pointer_types = [boolean8ptr, boolean8ptr2, nb.types.intp, nb.types.voidptr]
for _i, _p1 in enumerate(_pointer_types[:2]):
    for _j, _p2 in enumerate(_pointer_types):
        if _p1 == _p2 or _i > _j:
            continue
        typeconv.rules.default_type_manager.set_compatible(
            _p1, _p2, typeconv.Conversion.safe)
        typeconv.rules.default_type_manager.set_compatible(
            _p2, _p1, typeconv.Conversion.safe)


_ufunc_pos_args_match = re.compile(
    r'(?P<name>\w[\w\d_]*)\s*[(](?P<pos_args>[^/)]*)[/]?(?P<rest>.*)[)]').match
_req_opt_args_match = re.compile(r'(?P<req_args>[^[]*)(?P<opt_args>.*)').match
_annot_match = re.compile(
    r'\s*(?P<name>\w[\w\d_]*)\s*[:](?P<annotation>.*)').match


def get_signature(obj):
    if inspect.isfunction(obj):
        return inspect.signature(obj)
    if isinstance(obj, np.ufunc):
        parameters = dict()
        returns = dict()

        sigline = obj.__doc__.lstrip().splitlines(1)[0]
        m = _ufunc_pos_args_match(sigline)
        name = m['name']
        assert name == obj.__name__, (name, obj.__name__)

        # positional arguments
        m = _req_opt_args_match(m['pos_args'])
        req_pos_names, opt_pos_names = [], []
        for n in m.group('req_args').split(','):
            n = n.strip()
            if n:
                req_pos_names.append(n)
                parameters[n] = inspect.Parameter(
                    n, inspect.Parameter.POSITIONAL_ONLY)
        for n in (m.group('opt_args').replace('[', '')
                   .replace(']', '').split(',')):
            n = n.strip()
            if n:
                opt_pos_names.append(n)
                parameters[n] = inspect.Parameter(
                    n, inspect.Parameter.POSITIONAL_ONLY, default=None)
        # TODO: process non-positional arguments in `m['rest']`

        # scan for annotations and determine returns
        mode = 'none'
        for line in obj.__doc__.splitlines():
            if line in ['Parameters', 'Returns', 'Notes', 'See Also',
                        'Examples']:
                mode = line
                continue
            if mode == 'Parameters':
                m = _annot_match(line)
                if m is not None:
                    n = m['name']
                    if n in parameters:
                        annot = m['annotation'].strip()
                        parameters[n] = parameters[n].replace(
                            annotation=annot)
            if mode == 'Returns':
                m = _annot_match(line)
                if m is not None:
                    n = m['name']
                    annot = m['annotation'].strip()
                    returns[n] = annot

        sig = inspect.Signature(parameters=parameters.values())
        if len(returns) == 1:
            sig = sig.replace(return_annotation=list(returns.values())[0])
        elif returns:
            sig = sig.replace(return_annotation=returns)
        return sig

    raise NotImplementedError(obj)


# Import numba support

if 1:  # to avoid flake E402
    from . import structure_type
