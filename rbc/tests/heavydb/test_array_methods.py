import pytest
import numpy as np
from rbc.tests import heavydb_fixture
from rbc.heavydb import Array
from rbc.stdlib import array_api  # noqa: F401


rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


NUMERIC_TYPES = ['int8', 'int16', 'int32', 'int64', 'float32', 'float64']


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals()):
        define(o)
        yield o


ndarray_methods = [
    ('fill', (5, 4), [4.0, 4.0, 4.0, 4.0, 4.0]),
    ('max', (5, 4.0), 4.0),
    ('max_empty_int8', (0, ), np.iinfo(np.int8).min + 1),
    ('max_empty_int16', (0, ), np.iinfo(np.int16).min + 1),
    ('max_empty_int32', (0, ), np.iinfo(np.int32).min + 1),
    ('max_empty_int64', (0, ), np.iinfo(np.int64).min + 1),
    ('max_empty_float32', (0, ), np.finfo(np.float32).min),
    ('max_empty_float64', (0, ), np.finfo(np.float64).min),
    ('mean', (5, 2), 2.0),
    ('mean', (5, 2.0), 2.0),
    ('mean_empty_int8', (0, ), np.nan),
    ('mean_empty_int16', (0, ), np.nan),
    ('mean_empty_int32', (0, ), np.nan),
    ('mean_empty_int64', (0, ), np.nan),
    ('mean_empty_float32', (0, ), np.nan),
    ('mean_empty_float64', (0, ), np.nan),
    ('min', (5, 4.0), 4.0),
    ('min_empty_int8', (0, ), np.iinfo(np.int8).max),
    ('min_empty_int16', (0, ), np.iinfo(np.int16).max),
    ('min_empty_int32', (0, ), np.iinfo(np.int32).max),
    ('min_empty_int64', (0, ), np.iinfo(np.int64).max),
    ('min_empty_float32', (0, ), np.finfo(np.float32).max),
    ('min_empty_float64', (0, ), np.finfo(np.float64).max),
    ('sum', (5, 2.0), 10.0),
    ('prod', (5, 3.0), 243.0),
]


def define(heavydb):

    @heavydb('double[](int64, double)')
    def ndarray_fill(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a

    @heavydb('double(int64, double)')
    def ndarray_max(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.max()

    for retty in NUMERIC_TYPES:
        for op in ('min', 'max', 'mean'):
            fn_name = f'ndarray_{op}_empty_{retty}'
            fn = (f'def {fn_name}(size):\n'
                  f'    a = Array(size, "{retty}")\n'
                  f'    return a.{op}()\n')
            exec(fn)
            fn = locals()[fn_name]
            if op == 'mean':
                heavydb('float64(int32)')(fn)
            else:
                heavydb(f'{retty}(int32)')(fn)

    @heavydb('double(int64, double)')
    def ndarray_mean(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.mean()

    @heavydb('double(int64, double)')
    def ndarray_min(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.min()

    @heavydb('double(int64, double)')
    def ndarray_sum(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.sum()

    @heavydb('double(int64, double)')
    def ndarray_prod(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.prod()


@pytest.mark.parametrize("method, args, expected", ndarray_methods,
                         ids=[item[0] for item in ndarray_methods])
def test_ndarray_methods(heavydb, method, args, expected):
    query_args = ', '.join(map(str, args))
    query = f'SELECT ndarray_{method}({query_args})'

    _, result = heavydb.sql_execute(query)
    out = list(result)[0]

    if method == 'fill':
        assert np.array_equal(expected, out[0]), 'ndarray_' + method
    else:
        assert np.isclose(expected, out, equal_nan=True), \
            'ndarray_' + method
