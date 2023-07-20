import pytest
import numpy as np
from rbc.tests import heavydb_fixture
from rbc.stdlib import array_api


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), load_test_data=False):
        define(o)
        yield o


def define(heavydb):

    @heavydb('float64(T[])', T=['float64', 'int64', 'int32'])
    def array_api_std(x):
        return array_api.std(x)

    @heavydb('float64(T[])', T=['float64', 'int64', 'int32'])
    def array_api_var(x):
        return array_api.var(x)


@pytest.mark.parametrize('fn', ('std', 'var'))
@pytest.mark.parametrize('X', [
    [1, 2, 3],
    np.arange(1, 40, dtype=np.int32),
    np.arange(0.1, 1.0, 0.1, dtype=np.float64)
])
def test_std(heavydb, fn, X):

    caller = heavydb.get_caller(f'array_api_{fn}')
    got = caller(X).execute()
    expected = getattr(np, fn)(np.asarray(X))
    assert np.isclose(got, expected)
