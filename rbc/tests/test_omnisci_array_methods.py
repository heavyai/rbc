import pytest
import numpy as np


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    yield m


def test_ndarray_methods(omnisci):
    omnisci.reset()
    from rbc.omnisci_array import Array

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

    @omnisci('double(int64, double)')
    def ndarray_min(size, v):
        a = Array(size, 'double')
        a.fill(v)
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

    ndarray_methods = [
        ('fill', (5, 4), [4.0, 4.0, 4.0, 4.0, 4.0]),
        ('max', (5, 4.0), 4.0),
        ('max_initial', (5, 4.0, 30.0), 30.0),
        ('mean', (5, 2.0), 2.0),
        ('min', (5, 4.0), 4.0),
        ('min_initial', (5, 4.0, -3.0), -3.0),
        ('sum', (5, 2.0), 10.0),
        ('sum_initial', (5, 2.0, 2.0), 12.0),
        ('prod', (5, 3.0), 243.0),
        ('prod_initial', (5, 3.0, 2), 486.0),
    ]

    for method, args, expected in ndarray_methods:
        query = 'select ndarray_{method}'.format(**locals()) + \
            '(' + ', '.join(str(arg) for arg in args) + ')'
        _, result = omnisci.sql_execute(query)

        out = list(result)[0]

        if method == 'fill':
            assert np.array_equal(expected, out[0]), 'ndarray_' + method
        else:
            assert np.isclose(expected, out), 'ndarray_' + method
