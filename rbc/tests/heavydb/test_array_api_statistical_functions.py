import numpy as np
import pytest

from rbc.stdlib import array_api
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['arraynullrepeat']):
        define(o)
        yield o


def define(heavydb):
    T = ['double', 'float', 'int64', 'int32', 'int16', 'int8']

    @heavydb('T[](T[])', T=T)
    def drop_null(x):
        return x.drop_null()

    @heavydb('float64(T[], TextEncodingNone)', T=T)
    def array_api_stats(x, t):
        s = t.to_string()
        if s == 'std':
            return array_api.std(x)
        elif s == 'var':
            return array_api.var(x)
        elif s == 'min':
            return array_api.min(x)
        elif s == 'max':
            return array_api.max(x)
        elif s == 'mean':
            return array_api.mean(x)
        elif s == 'prod':
            return array_api.prod(x)
        else:  # sum
            return array_api.sum(x)


@pytest.mark.parametrize('fn', ('min', 'max', 'var', 'std', 'sum', 'prod', 'mean'))
@pytest.mark.parametrize('col', ('f8', 'f4', 'i8', 'i4', 'i2', 'i1'))
def test_stats(heavydb, fn, col):

    table = heavydb.table_name + 'arraynullrepeat'
    query = f"SELECT drop_null({col}), array_api_stats({col}, '{fn}') FROM {table}"
    _, r = heavydb.sql_execute(query)
    for val, got in r:
        if val is None or val == []:
            if fn in ('mean', 'std', 'var'):
                assert np.isnan(got)
            elif fn == 'sum':
                assert got == 0.0
            elif fn == 'prod':
                assert got == 1.0
        else:
            x = np.asarray(val)
            expected = getattr(np, fn)(x)
            assert np.isclose(got, expected)
