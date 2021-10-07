import pytest
import numpy as np
from rbc.tests import omnisci_fixture
from rbc.omnisci_backend import Array


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals()):
        define(o)
        yield o


ndarray_methods = [
    ('fill', (5, 4), [4.0, 4.0, 4.0, 4.0, 4.0]),
    ('max', (5, 4.0), 4.0),
    ('max_empty', (0, ), -128),
    ('max_initial', (5, 4.0, 30.0), 30.0),
    ('mean', (5, 2.0), 2.0),
    ('mean_empty_float', (0, ), np.nan),
    ('mean_empty_int', (0, ), 0),
    ('min', (5, 4.0), 4.0),
    ('min_empty', (0, ), 32767),
    ('min_initial', (5, 4.0, -3.0), -3.0),
    ('sum', (5, 2.0), 10.0),
    ('sum_initial', (5, 2.0, 2.0), 12.0),
    ('prod', (5, 3.0), 243.0),
    ('prod_initial', (5, 3.0, 2), 486.0),
]


def define(omnisci):

    @omnisci('double[](int64, double)')
    def ndarray_fill(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a

    @omnisci('double(int64, double)')
    def ndarray_max(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.max()

    @omnisci('int8(int32)')
    def ndarray_max_empty(size):
        a = Array(size, 'int8')
        return a.max()

    @omnisci('double(int64, double, double)')
    def ndarray_max_initial(size, v, initial):
        a = Array(size, 'double')
        a.fill(v)
        return a.max(initial=initial)

    @omnisci('double(int64, double)')
    def ndarray_mean(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.mean()

    @omnisci('float64(int64)')
    def ndarray_mean_empty_float(size):
        a = Array(size, 'float64')
        return a.mean()

    @omnisci('float64(int64)')
    def ndarray_mean_empty_int(size):
        a = Array(size, 'int32')
        return a.mean()

    @omnisci('double(int64, double)')
    def ndarray_min(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.min()

    @omnisci('int16(int64)')
    def ndarray_min_empty(size):
        a = Array(size, 'int16')
        return a.min()

    @omnisci('double(int64, double, double)')
    def ndarray_min_initial(size, v, initial):
        a = Array(size, 'double')
        a.fill(v)
        return a.min(initial=initial)

    @omnisci('double(int64, double)')
    def ndarray_sum(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.sum()

    @omnisci('double(int64, double, double)')
    def ndarray_sum_initial(size, v, initial):
        a = Array(size, 'double')
        a.fill(v)
        return a.sum(initial=initial)

    @omnisci('double(int64, double)')
    def ndarray_prod(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.prod()

    @omnisci('double(int64, double, double)')
    def ndarray_prod_initial(size, v, initial):
        a = Array(size, 'double')
        a.fill(v)
        return a.prod(initial=initial)


@pytest.mark.parametrize("method, args, expected", ndarray_methods,
                         ids=[item[0] for item in ndarray_methods])
def test_ndarray_methods(omnisci, method, args, expected):
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

    query_args = ', '.join(map(str, args))
    query = f'SELECT ndarray_{method}({query_args})'

    _, result = omnisci.sql_execute(query)
    out = list(result)[0]

    if method == 'fill':
        assert np.array_equal(expected, out[0]), 'ndarray_' + method
    else:
        assert np.isclose(expected, out, equal_nan=True), \
            'ndarray_' + method
