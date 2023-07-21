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
    @heavydb('int64[](T[])', T=['double', 'int64'])
    def nonzero(arr):
        return array_api.nonzero(arr)

    @heavydb('int64[](T[])', T=['double', 'int64'])
    def argmin(arr):
        return array_api.argmin(arr)

    @heavydb('int64[](T[])', T=['double', 'int64'])
    def argmax(arr):
        return array_api.argmax(arr)


# @pytest.mark.xfail
@pytest.mark.parametrize('fn', ('nonzero', 'argmin', 'argmax'))
@pytest.mark.parametrize('col', ('f8', 'i8'))
def test_nonzero_argmin_argmax(heavydb, fn, col):

    table = heavydb.table_name + 'arraynullrepeat'
    query = f"SELECT {col}, {fn}({col}) FROM {table}"
    _, r = heavydb.sql_execute(query)
    for val, got in r:
        if val is None or val == []:
            assert got is None
        else:
            x = np.asarray(val)
            x = x[x != None]
            if fn == 'nonzero':
                expected = x.nonzero()[0]
            elif fn == 'argmin':
                expected = x.argmin()
            else:
                expected = x.argmax()

            np.testing.assert_array_equal(expected, got)