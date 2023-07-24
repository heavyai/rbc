import pytest
import numpy as np
from rbc.tests import heavydb_fixture
from rbc.stdlib import array_api


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['arraynullrepeat']):
        define(o)
        yield o


def define(heavydb):
    T = ['double', 'float', 'int64', 'int32', 'int16', 'int8']

    @heavydb('int64[](T[])', T=T)
    def nonzero(arr):
        return array_api.nonzero(arr)

    @heavydb('int64[](T[])', T=T)
    def argmin(arr):
        return array_api.argmin(arr)

    @heavydb('int64[](T[])', T=T)
    def argmax(arr):
        return array_api.argmax(arr)


@pytest.mark.parametrize('fn', ('nonzero', 'argmin', 'argmax'))
@pytest.mark.parametrize('col', ('f8', 'f4', 'i8', 'i4', 'i2'))
def test_nonzero_argmin_argmax(heavydb, fn, col):

    table = heavydb.table_name + 'arraynullrepeat'
    query = f"SELECT {col}, {fn}({col}) FROM {table}"
    _, r = heavydb.sql_execute(query)
    for val, got in r:
        if val is None or val == []:
            assert got is None
        else:
            x = np.asarray(val)
            if fn == 'nonzero':
                x[x == None] = 0  # noqa: E711
                expected = x.nonzero()[0]
            elif fn == 'argmin':
                x[x == None] = 1234  # noqa: E711
                expected = x.argmin()
            else:
                x[x == None] = -123  # noqa: E711
                expected = x.argmax()

            np.testing.assert_array_equal(expected, got)
