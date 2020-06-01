import pytest
import numpy as np
from rbc.omnisci_array import Array
from numba import types as nb_types


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    yield m


operator_methods = [
    ('eq', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 1, 0, 0]),
    ('ne', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 0, 1, 1]),
    ('lt', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 0, 0, 0]),
    ('le', 'int8[](int64, int32)', (6, 3), [1, 1, 1, 1, 0, 0]),
    ('gt', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 0, 1, 1]),
    ('ge', 'int8[](int64, int32)', (6, 3), [0, 0, 0, 1, 1, 1]),
    ('in', 'int8(int64, int32)', (6, 3), True),
    ('not_in', 'int8(int64, int32)', (6, 3), False),
    ('is', 'int8(int64, int32)', (6, 3), True),
    ('is_not', 'int8(int64, int32)', (6, 3), False),
    ('is_not2', 'int8(int64, int32)', (6, 3), True),
]


def operator_eq(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a == v


def operator_ne(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a != v


def operator_lt(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a < v


def operator_le(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a <= v


def operator_gt(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a > v


def operator_ge(size, v):
    a = Array(size, 'int32')
    for i in range(size):
        a[i] = nb_types.int32(i)
    return a >= v


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
    else:
        assert np.array_equal(expected, out[0]), 'operator_' + suffix
