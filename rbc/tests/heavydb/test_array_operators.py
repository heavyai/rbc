import pytest
import numpy as np
from rbc.heavydb import Array
from rbc.tests import heavydb_fixture
from numba import types as nb_types
import operator


rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals()):
        define(o)
        yield o


operator_methods = [
    ('abs', (6,), np.arange(6)),
    ('add', (6,), np.full(6, 5)),
    ('and_bw', (6,), [0, 0, 2, 2, 0, 0]),
    ('countOf', (6, 3, 4), 0),
    ('countOf', (6, 3, 3), 6),
    ('eq', (6, 3), [0, 0, 0, 1, 0, 0]),
    ('eq_array', (6, 3), True),
    ('floordiv', (6,), [3, 2, 2, 2, 2, 1]),
    ('floordiv2', (6,), [3.0, 2.0, 2.0, 2.0, 2.0, 1.0]),
    ('ge', (6, 3), [0, 0, 0, 1, 1, 1]),
    ('ge_array', (6, 3), True),
    ('gt', (6, 3), [0, 0, 0, 0, 1, 1]),
    ('gt_array', (6, 3), False),
    ('iadd', (6,), [1, 2, 3, 4, 5, 6]),
    ('iand', (6,), [0, 0, 2, 2, 0, 0]),
    ('ifloordiv', (6,), [3, 2, 2, 2, 2, 1]),
    ('ifloordiv2', (6,), [3, 2, 2, 2, 2, 1]),
    ('ilshift', (6,), [0, 16, 16, 12, 8, 5]),
    ('imul', (6,), [0, 4, 6, 6, 4, 0]),
    ('ior', (6,), [5, 5, 3, 3, 5, 5]),
    ('isub', (6,), [-5, -3, -1, 1, 3, 5]),
    ('ipow', (6,), [1, 32, 81, 64, 25, 6]),
    ('irshift', (6,), [0, 0, 0, 0, 2, 5]),
    ('itruediv', (6,), [3, 2, 2, 2, 2, 1]),
    ('itruediv2', (6,), [3.3333333333333335, 2.75, 2.4, 2.1666666666666665, 2.0, 1.875]),  # noqa: E501
    ('imod', (6,), [0, 4, 1, 5, 2, 6]),
    ('ixor', (6,), [5, 5, 1, 1, 5, 5]),
    ('in', (6, 3), True),
    ('is', (6, 3), True),
    ('is_not', (6, 3), False),
    ('is_not2', (6, 3), True),
    ('le', (6, 3), [1, 1, 1, 1, 0, 0]),
    ('le_array', (6, 3), True),
    ('lshift', (6,), [0, 16, 16, 12, 8, 5]),
    ('lt', (6, 3), [1, 1, 1, 0, 0, 0]),
    ('lt_array', (6, 3), False),
    ('mul', (6,), [0, 4, 6, 6, 4, 0]),
    ('mod', (6,), [0, 4, 1, 5, 2, 6]),
    ('ne', (6, 3), [1, 1, 1, 0, 1, 1]),
    ('ne_array', (6, 3), False),
    ('neg', (6,), [0, -1, -2, -3, -4, -5]),
    ('not_in', (6, 3), False),
    ('or_bw', (6,), [5, 5, 3, 3, 5, 5]),
    ('pos', (6,), [0, -1, -2, -3, -4, -5]),
    ('pow', (6,), [1, 32, 81, 64, 25, 6]),
    ('rshift', (6,), [0, 0, 0, 0, 2, 5]),
    ('sub', (6,), [-5, -3, -1, 1, 3, 5]),
    ('truediv', (6,), [3, 2, 2, 2, 2, 1]),
    ('truediv2', (6,), [3.3333333333333335, 2.75, 2.4, 2.1666666666666665, 2.0, 1.875]),  # noqa: E501
    ('xor', (6,), [5, 5, 1, 1, 5, 5]),
]


def define(heavydb):

    @heavydb('int32[](int64)')
    def operator_abs(size):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(-i)
        return abs(a)

    @heavydb('int32[](int64)')
    def operator_add(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.add(a, b)

    @heavydb('int32[](int64)')
    def operator_and_bw(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.and_(a, b)

    @heavydb('int64(int64, int64, int64)')
    def operator_countOf(size, fill_value, b):
        a = Array(size, 'int64')
        for i in range(size):
            a[i] = fill_value
        return operator.countOf(a, b)

    @heavydb('int8[](int64, int32)')
    def operator_eq(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a == v

    @heavydb('bool(int64, int32)')
    def operator_eq_array(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a == a

    @heavydb('int32[](int64)')
    def operator_floordiv(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i+10)
            b[i] = nb_types.int32(i+3)
        return operator.floordiv(a, b)

    @heavydb('double[](int64)')
    def operator_floordiv2(size):
        a = Array(size, 'double')
        b = Array(size, 'double')
        for i in range(size):
            a[i] = nb_types.double(i+10)
            b[i] = nb_types.double(i+3)
        return operator.floordiv(a, b)

    @heavydb('int8[](int64, int32)')
    def operator_ge(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a >= v

    @heavydb('bool(int64, int32)')
    def operator_ge_array(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a >= a

    @heavydb('int8[](int64, int32)')
    def operator_gt(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a > v

    @heavydb('bool(int64, int32)')
    def operator_gt_array(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a > a

    @heavydb('int32[](int64)')
    def operator_iadd(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(1)
        operator.iadd(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_iand(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.iand(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_ifloordiv(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i+10)
            b[i] = nb_types.int32(i+3)
        operator.ifloordiv(a, b)
        return a

    @heavydb('double[](int64)')
    def operator_ifloordiv2(size):
        a = Array(size, 'double')
        b = Array(size, 'double')
        for i in range(size):
            a[i] = nb_types.double(i+10)
            b[i] = nb_types.double(i+3)
        operator.ifloordiv(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_ilshift(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.ilshift(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_imul(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.imul(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_ior(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.ior(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_isub(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.isub(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_ipow(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i+1)
            b[i] = nb_types.int32(size-i)
        operator.ipow(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_irshift(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.irshift(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_itruediv(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i+10)
            b[i] = nb_types.int32(i+3)
        operator.itruediv(a, b)
        return a

    @heavydb('double[](int64)')
    def operator_itruediv2(size):
        a = Array(size, 'double')
        b = Array(size, 'double')
        for i in range(size):
            a[i] = nb_types.double(i+10)
            b[i] = nb_types.double(i+3)
        operator.itruediv(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_imod(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i * 123)
            b[i] = nb_types.int32(7)
        operator.imod(a, b)
        return a

    @heavydb('int32[](int64)')
    def operator_ixor(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        operator.ixor(a, b)
        return a

    @heavydb('int8(int64, int32)')
    def operator_in(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return v in a

    @heavydb('int8(int64, int32)')
    def operator_is(size, v):
        a = Array(size, 'int32')
        a.fill(v)
        return a is a

    @heavydb('int8(int64, int32)')
    def operator_is_not(size, v):
        a = Array(size, 'int32')
        a.fill(v)
        return a is not a

    @heavydb('int8(int64, int32)')
    def operator_is_not2(size, v):
        a = Array(size, 'int32')
        a.fill(v)
        b = Array(size, 'int32')
        b.fill(v)
        return a is not b

    @heavydb('int8[](int64, int32)')
    def operator_le(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a <= v

    @heavydb('bool(int64, int32)')
    def operator_le_array(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a <= a

    @heavydb('int32[](int64)')
    def operator_lshift(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.lshift(a, b)

    @heavydb('int8[](int64, int32)')
    def operator_lt(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a < v

    @heavydb('bool(int64, int32)')
    def operator_lt_array(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a < a

    @heavydb('int32[](int64)')
    def operator_mul(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.mul(a, b)

    @heavydb('int32[](int64)')
    def operator_mod(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i * 123)
            b[i] = nb_types.int32(7)
        return operator.mod(a, b)

    @heavydb('int8[](int64, int32)')
    def operator_ne(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a != v

    @heavydb('bool(int64, int32)')
    def operator_ne_array(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return a != a

    @heavydb('int32[](int64)')
    def operator_neg(size):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return operator.neg(a)

    @heavydb('int8(int64, int32)')
    def operator_not_in(size, v):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
        return v not in a

    @heavydb('int32[](int64)')
    def operator_or_bw(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.or_(a, b)

    @heavydb('int32[](int64)')
    def operator_pos(size):
        a = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(-i)
        return operator.pos(a)

    @heavydb('int32[](int64)')
    def operator_pow(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i+1)
            b[i] = nb_types.int32(size-i)
        return operator.pow(a, b)

    @heavydb('int32[](int64)')
    def operator_rshift(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.rshift(a, b)

    @heavydb('int32[](int64)')
    def operator_sub(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.sub(a, b)

    @heavydb('int32[](int64)')
    def operator_truediv(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i+10)
            b[i] = nb_types.int32(i+3)
        return operator.truediv(a, b)

    @heavydb('double[](int64)')
    def operator_truediv2(size):
        a = Array(size, 'double')
        b = Array(size, 'double')
        for i in range(size):
            a[i] = nb_types.double(i+10)
            b[i] = nb_types.double(i+3)
        return operator.truediv(a, b)

    @heavydb('int32[](int64)')
    def operator_xor(size):
        a = Array(size, 'int32')
        b = Array(size, 'int32')
        for i in range(size):
            a[i] = nb_types.int32(i)
            b[i] = nb_types.int32(size-i-1)
        return operator.xor(a, b)


@pytest.mark.parametrize("suffix, args, expected", operator_methods,
                         ids=[item[0] for item in operator_methods])
def test_array_operators(heavydb, suffix, args, expected):
    query = 'select operator_{suffix}'.format(**locals()) + \
            '(' + ', '.join(map(str, args)) + ')'
    _, result = heavydb.sql_execute(query)
    out = list(result)[0]

    if suffix in ['in', 'not_in']:
        assert (expected == out[0]), 'operator_' + suffix
    elif '_array' in suffix:
        assert (expected == out[0]), 'operator_' + suffix
    else:
        assert np.array_equal(expected, out[0]), 'operator_' + suffix
