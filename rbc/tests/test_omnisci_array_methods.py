import pytest
import numpy as np
from rbc.omnisci_backend import Array


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    # issue https://github.com/xnd-project/rbc/issues/134
    table_name = 'rbc_test_omnisci_array_methods'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    yield m


ndarray_methods = [
    ('fill', 'double[](int64, double)', (5, 4), [4.0, 4.0, 4.0, 4.0, 4.0]),
    ('max', 'double(int64, double)', (5, 4.0), 4.0),
    ('max_empty', 'int8(int32)', (0, ), -128),
    ('max_initial', 'double(int64, double, double)', (5, 4.0, 30.0), 30.0),
    ('mean', 'double(int64, double)', (5, 2.0), 2.0),
    ('mean_empty_float', 'float64(int64)', (0, ), np.nan),
    ('mean_empty_int', 'int32(int64)', (0, ), 0),
    ('min', 'double(int64, double)', (5, 4.0), 4.0),
    ('min_empty', 'int16(int64)', (0, ), 32767),
    ('min_initial', 'double(int64, double, double)', (5, 4.0, -3.0), -3.0),
    ('sum', 'double(int64, double)', (5, 2.0), 10.0),
    ('sum_initial', 'double(int64, double, double)', (5, 2.0, 2.0), 12.0),
    ('prod', 'double(int64, double)', (5, 3.0), 243.0),
    ('prod_initial', 'double(int64, double, double)', (5, 3.0, 2), 486.0),
]


def ndarray_fill(size, v):
    a = Array(size, 'double')
    a.fill(v)
    return a


def ndarray_max(size, v):
    a = Array(size, 'double')
    a.fill(v)
    return a.max()


def ndarray_max_empty(size):
    a = Array(size, 'int8')
    return a.max()


def ndarray_max_initial(size, v, initial):
    a = Array(size, 'double')
    a.fill(v)
    return a.max(initial=initial)


def ndarray_mean(size, v):
    a = Array(size, 'double')
    a.fill(v)
    return a.mean()


def ndarray_mean_empty_float(size):
    a = Array(size, 'float64')
    return a.mean()


def ndarray_mean_empty_int(size):
    a = Array(size, 'int32')
    return a.mean()


def ndarray_min(size, v):
    a = Array(size, 'double')
    a.fill(v)
    return a.min()


def ndarray_min_empty(size):
    a = Array(size, 'int16')
    return a.min()


def ndarray_min_initial(size, v, initial):
    a = Array(size, 'double')
    a.fill(v)
    return a.min(initial=initial)


def ndarray_sum(size, v):
    a = Array(size, 'double')
    a.fill(v)
    return a.sum()


def ndarray_sum_initial(size, v, initial):
    a = Array(size, 'double')
    a.fill(v)
    return a.sum(initial=initial)


def ndarray_prod(size, v):
    a = Array(size, 'double')
    a.fill(v)
    return a.prod()


def ndarray_prod_initial(size, v, initial):
    a = Array(size, 'double')
    a.fill(v)
    return a.prod(initial=initial)


@pytest.mark.parametrize("method, signature, args, expected", ndarray_methods,
                         ids=[item[0] for item in ndarray_methods])
def test_ndarray_methods(omnisci, method, signature, args, expected):
    omnisci.reset()

    if omnisci.has_cuda and omnisci.version < (5, 5):
        pytest.skip(
            f'{method}: crashes CUDA enabled omniscidb server [issue 93]')

    if available_version[:3] == (5, 3, 1) and method in ['fill']:
        pytest.skip(
            f'{method}: crashes CPU-only omniscidb server v 5.3.1 [issue 113]')

    if available_version[:3] >= (5, 3, 1) and method in ['max_empty']:
        pytest.skip(
            f'{method}: fails on CPU-only omniscidb server'
            ' v 5.3.1+ [issue 114]')

    omnisci(signature)(eval('ndarray_{}'.format(method)))

    query = 'select ndarray_{method}'.format(**locals()) + \
            '(' + ', '.join(map(str, args)) + ')'
    _, result = omnisci.sql_execute(query)
    out = list(result)[0]

    if method == 'fill':
        assert np.array_equal(expected, out[0]), 'ndarray_' + method
    else:
        assert np.isclose(expected, out, equal_nan=True), \
            'ndarray_' + method
