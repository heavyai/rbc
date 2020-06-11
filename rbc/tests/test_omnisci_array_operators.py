import pytest
import numpy as np
from rbc.omnisci_array import Array
from numba import types as nb_types
import operator


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    yield m


operator_methods = [
    ('add', 'int32[](int64)', (6,), np.full(6, 5)),
    ('and_', 'int32[](int64)', (6,), [0, 0, 2, 2, 0, 0]),
    ('eq', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 1, 0, 0]),
    ('eq_array', 'bool(int64, int32)', (6, 3), True),
    ('floordiv', 'int32[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('floordiv2', 'double[](int64)', (6,), [3.0, 2.0, 2.0, 2.0, 2.0, 1.0]),
    ('ge', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 1, 1, 1]),
    ('ge_array', 'bool(int64, int32)', (6, 3), True),
    ('gt', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 0, 1, 1]),
    ('gt_array', 'bool(int64, int32)', (6, 3), False),
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
    ('ne', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 0, 1, 1]),
    ('ne_array', 'bool(int64, int32)', (6, 3), False),
    ('not_in', 'int8(int64, int32)', (6, 3), False),
    ('or_', 'int32[](int64)', (6,), [5, 5, 3, 3, 5, 5]),
    ('pow', 'int32[](int64)', (6,), [1, 32, 81, 64, 25, 6]),
    ('rshift', 'int32[](int64)', (6,), [0, 0, 0, 0, 2, 5]),
    ('sub', 'int32[](int64)', (6,), [-5, -3, -1, 1, 3, 5]),
    ('truediv', 'int32[](int64)', (6,), [3, 2, 2, 2, 2, 1]),
    ('truediv2', 'double[](int64)', (6,), [3.3333333333333335, 2.75, 2.4, 2.1666666666666665, 2.0, 1.875]),  # noqa: E501
    ('xor', 'int32[](int64)', (6,), [5, 5, 1, 1, 5, 5]),
]


def operator_rshift(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.rshift(a, b)


def operator_lshift(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.lshift(a, b)


def operator_floordiv(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+10)
        b[i] = nb_types.int32(i+3)
    return operator.floordiv(a, b)


def operator_floordiv2(size):
    a = Array(size, 'double')
    b = Array(size, 'double')
    for i in range(size):
        a[i] = nb_types.double(i+10)
        b[i] = nb_types.double(i+3)
    return operator.floordiv(a, b)


def operator_truediv(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+10)
        b[i] = nb_types.int32(i+3)
    return operator.truediv(a, b)


def operator_truediv2(size):
    a = Array(size, 'double')
    b = Array(size, 'double')
    for i in range(size):
        a[i] = nb_types.double(i+10)
        b[i] = nb_types.double(i+3)
    return operator.truediv(a, b)


def operator_pow(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i+1)
        b[i] = nb_types.int32(size-i)
    return operator.pow(a, b)


def operator_mul(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.mul(a, b)


def operator_sub(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.sub(a, b)


def operator_add(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.add(a, b)


def operator_xor(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.xor(a, b)


def operator_or_(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.or_(a, b)


def operator_and_(size):
    a = Array(size, 'int32')
    b = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
        b[i] = nb_types.int32(size-i-1)
    return operator.and_(a, b)


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
