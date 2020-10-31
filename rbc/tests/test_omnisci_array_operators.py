import pytest
import numpy as np
from rbc.omnisci_backend import Array
from numba import types as nb_types
import operator


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    # issue https://github.com/xnd-project/rbc/issues/134
    table_name = 'rbc_test_omnisci_array_operators'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    yield m


operator_methods = [
    ('abs', 'int32[](int64)', (6,), np.arange(6)),
    ('add', 'int32[](int64)', (6,), np.full(6, 5)),
    ('and_bw', 'int32[](int64)', (6,), [0, 0, 2, 2, 0, 0]),
    ('countOf', 'int64(int64, int64, int64)', (6, 3, 4), 0),
    ('countOf', 'int64(int64, int64, int64)', (6, 3, 3), 6),
    ('eq', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 1, 0, 0]),
    ('eq_array', 'bool(int64, int32)', (6, 3), True),
    ('floordiv', 'int32[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('floordiv2', 'double[](int64)', (6,), [3.0, 2.0, 2.0, 2.0, 2.0, 1.0]),
    ('ge', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 1, 1, 1]),
    ('ge_array', 'bool(int64, int32)', (6, 3), True),
    ('gt', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 0, 1, 1]),
    ('gt_array', 'bool(int64, int32)', (6, 3), False),
    ('iadd', 'int32[](int64)', (6,), [1, 2, 3, 4, 5, 6]),
    ('iand', 'int32[](int64)', (6,), [0, 0, 2, 2, 0, 0]),
    ('ifloordiv', 'int32[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('ifloordiv2', 'double[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('ilshift', 'int32[](int64)', (6,), [0, 16, 16, 12, 8, 5]),
    ('imul', 'int32[](int64)', (6,), [0, 4, 6, 6, 4, 0]),
    ('ior', 'int32[](int64)', (6,), [5, 5, 3, 3, 5, 5]),
    ('isub', 'int32[](int64)', (6,), [-5, -3, -1, 1, 3, 5]),
    ('ipow', 'int32[](int64)', (6,), [1, 32, 81, 64, 25, 6]),
    ('irshift', 'int32[](int64)', (6,), [0, 0, 0, 0, 2, 5]),
    ('itruediv', 'int32[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('itruediv2', 'double[](int64)', (6,), [3.3333333333333335, 2.75, 2.4, 2.1666666666666665, 2.0, 1.875]),  # noqa: E501
    ('imod', 'int32[](int64)', (6,), [0, 4, 1, 5, 2, 6]),
    ('ixor', 'int32[](int64)', (6,), [5, 5, 1, 1, 5, 5]),
    ('in', 'int8(int64, int32)', (6, 3), True),
    ('is', 'int8(int64, int32)', (6, 3), True),
    ('is_not', 'int8(int64, int32)', (6, 3), False),
    ('is_not2', 'int8(int64, int32)', (6, 3), True),
    ('le', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 1, 0, 0]),
    ('le_array', 'bool(int64, int32)', (6, 3), True),
    ('lshift', 'int32[](int64)', (6,), [0, 16, 16, 12, 8, 5]),
    ('lt', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 0, 0, 0]),
    ('lt_array', 'bool(int64, int32)', (6, 3), False),
    ('mul', 'int32[](int64)', (6,), [0, 4, 6, 6, 4, 0]),
    ('mod', 'int32[](int64)', (6,), [0, 4, 1, 5, 2, 6]),
    ('ne', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 0, 1, 1]),
    ('neg', 'int32[](int64)', (6,), [0, -1, -2, -3, -4, -5]),
    ('ne_array', 'bool(int64, int32)', (6, 3), False),
    ('not_in', 'int8(int64, int32)', (6, 3), False),
    ('or_bw', 'int32[](int64)', (6,), [5, 5, 3, 3, 5, 5]),
    ('pos', 'int32[](int64)', (6,), [0, -1, -2, -3, -4, -5]),
    ('pow', 'int32[](int64)', (6,), [1, 32, 81, 64, 25, 6]),
    ('rshift', 'int32[](int64)', (6,), [0, 0, 0, 0, 2, 5]),
    ('sub', 'int32[](int64)', (6,), [-5, -3, -1, 1, 3, 5]),
    ('truediv', 'int32[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('truediv2', 'double[](int64)', (6,), [3.3333333333333335, 2.75, 2.4, 2.1666666666666665, 2.0, 1.875]),  # noqa: E501
    ('xor', 'int32[](int64)', (6,), [5, 5, 1, 1, 5, 5]),
]


def operator_countOf(size, fill_value, b):
    a = Array(size, 'int64')
    for i in range(size):
        a[i] = fill_value
    return operator.countOf(a, b)


def operator_neg(size):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return operator.neg(a)


def operator_abs(size):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(-i)
    return abs(a)


def operator_pos(size):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(-i)
    return operator.pos(a)


def operator_rshift(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.rshift(a, b)


def operator_irshift(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.irshift(a, b)
    return a


def operator_lshift(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.lshift(a, b)


def operator_ilshift(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.ilshift(a, b)
    return a


def operator_floordiv(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+10)
        b[i] = nb_types.int32(i+3)
    return operator.floordiv(a, b)


def operator_ifloordiv(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+10)
        b[i] = nb_types.int32(i+3)
    operator.ifloordiv(a, b)
    return a


def operator_floordiv2(size):
    a = Array(size, 'double')
    b = Array(size, 'double')
    for i in range(size):
        a[i] = nb_types.double(i+10)
        b[i] = nb_types.double(i+3)
    return operator.floordiv(a, b)


def operator_ifloordiv2(size):
    a = Array(size, 'double')
    b = Array(size, 'double')
    for i in range(size):
        a[i] = nb_types.double(i+10)
        b[i] = nb_types.double(i+3)
    operator.ifloordiv(a, b)
    return a


def operator_truediv(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+10)
        b[i] = nb_types.int32(i+3)
    return operator.truediv(a, b)


def operator_itruediv(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+10)
        b[i] = nb_types.int32(i+3)
    operator.itruediv(a, b)
    return a


def operator_truediv2(size):
    a = Array(size, 'double')
    b = Array(size, 'double')
    for i in range(size):
        a[i] = nb_types.double(i+10)
        b[i] = nb_types.double(i+3)
    return operator.truediv(a, b)


def operator_itruediv2(size):
    a = Array(size, 'double')
    b = Array(size, 'double')
    for i in range(size):
        a[i] = nb_types.double(i+10)
        b[i] = nb_types.double(i+3)
    operator.itruediv(a, b)
    return a


def operator_pow(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+1)
        b[i] = nb_types.int32(size-i)
    return operator.pow(a, b)


def operator_ipow(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+1)
        b[i] = nb_types.int32(size-i)
    operator.ipow(a, b)
    return a


def operator_mul(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.mul(a, b)


def operator_imul(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.imul(a, b)
    return a


def operator_mod(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i * 123)
        b[i] = nb_types.int32(7)
    return operator.mod(a, b)


def operator_imod(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i * 123)
        b[i] = nb_types.int32(7)
    operator.imod(a, b)
    return a


def operator_sub(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.sub(a, b)


def operator_isub(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.isub(a, b)
    return a


def operator_add(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.add(a, b)


def operator_iadd(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(1)
    operator.iadd(a, b)
    return a


def operator_xor(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.xor(a, b)


def operator_ixor(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.ixor(a, b)
    return a


def operator_or_bw(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.or_(a, b)


def operator_ior(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.ior(a, b)
    return a


def operator_and_bw(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.and_(a, b)


def operator_iand(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    operator.iand(a, b)
    return a


def operator_eq(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a == v


def operator_eq_array(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a == a


def operator_ne(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a != v


def operator_ne_array(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a != a


def operator_lt(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a < v


def operator_lt_array(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a < a


def operator_le(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a <= v


def operator_le_array(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a <= a


def operator_gt(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a > v


def operator_gt_array(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a > a


def operator_ge(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a >= v


def operator_ge_array(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a >= a


def operator_in(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return v in a


def operator_not_in(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return v not in a


def operator_is(size, v):
    a = Array(size, 'int32')
    a.fill(v)
    return a is a


def operator_is_not(size, v):
    a = Array(size, 'int32')
    a.fill(v)
    return a is not a


def operator_is_not2(size, v):
    a = Array(size, 'int32')
    a.fill(v)
    b = Array(size, 'int32')
    b.fill(v)
    return a is not b


@pytest.mark.parametrize("suffix, signature, args, expected", operator_methods,
                         ids=[item[0] for item in operator_methods])
def test_array_operators(omnisci, suffix, signature, args, expected):

    if omnisci.has_cuda and suffix in ['countOf', 'in', 'not_in'] and omnisci.version < (5, 5):
        # https://github.com/xnd-project/rbc/issues/107
        pytest.skip(f'operator_{suffix}: crashes CUDA enabled omniscidb server'
                    ' [rbc issue 107]')

    if (available_version[:3] == (5, 3, 1)
        and suffix in ['abs', 'add', 'and_bw', 'eq', 'floordiv', 'floordiv2',
                       'ge', 'gt', 'iadd', 'iand', 'ifloordiv', 'ifloordiv2',
                       'ilshift', 'imul', 'ior', 'isub', 'ipow', 'irshift',
                       'itruediv', 'itruediv2', 'imod', 'ixor', 'le', 'lshift',
                       'lt', 'mul', 'mod', 'ne', 'neg', 'or_bw', 'pos', 'pow',
                       'rshift', 'sub', 'truediv', 'truediv2', 'xor']):
        pytest.skip(
            f'operator_{suffix}: crashes CPU-only omniscidb server v 5.3.1'
            ' [issue 115]')

    omnisci.reset()

    omnisci(signature)(eval('operator_{}'.format(suffix)))

    query = 'select operator_{suffix}'.format(**locals()) + \
            '(' + ', '.join(map(str, args)) + ')'
    _, result = omnisci.sql_execute(query)
    out = list(result)[0]

    if suffix in ['in', 'not_in']:
        assert (expected == out[0]), 'operator_' + suffix
    elif '_array' in suffix:
        assert (expected == out[0]), 'operator_' + suffix
    else:
        assert np.array_equal(expected, out[0]), 'operator_' + suffix
