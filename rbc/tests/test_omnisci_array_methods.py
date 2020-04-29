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


    # @omnisci('double(int64, bool)')
    # def ndarray_any(size, v):
    #     a = Array(size, 'bool')
    #     a.fill(v)
    #     return a.any()


    # @omnisci('double(int64, bool)')
    # def ndarray_all(size, v):
    #     a = Array(size, 'bool')
    #     a.fill(v)
    #     return a.all()

    # @omnisci('double(int64, double)')
    # def ndarray_fill(size, v):
    #     a = Array(size, 'double')
    #     a.fill(v)
    #     return a


    @omnisci('double(int64, double)')
    def ndarray_max(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.max()


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


    @omnisci('double(int64, double)')
    def ndarray_sum(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.sum()


    @omnisci('double(int64, double)')
    def ndarray_prod(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.prod()

    ndarray_methods = [
        # ('any', (5, 0), 0),
        # ('any', (5, 1), 1),
        # ('all', (5, 0), 0),
        # ('all', (5, 1), 1),
        # ('fill', (5, 4.0), [4.0, 4.0, 4.0, 4.0, 4.0]),
        ('max', (5, 4.0), 4.0),
        ('mean', (5, 2.0), 2.0),
        ('min', (5, 4.0), 4.0),
        ('sum', (5, 2.0), 10.0),
        ('prod', (5, 3.0), 243.0),
    ]


    for method, args, expected in ndarray_methods:
        query = 'select ndarray_{method}'.format(**locals()) + \
            '(' + ', '.join(str(arg) for arg in args) + ')'
        _, result = omnisci.sql_execute(query)

        out = list(result)[0]

        assert(np.isclose(expected, out))
