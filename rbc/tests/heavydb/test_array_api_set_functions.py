import pytest
from rbc.tests import heavydb_fixture
from rbc.stdlib import array_api


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['arraynullrepeat']):
        define(o)
        yield o


def define(heavydb):
    @heavydb('T[](T[])', T=['double', 'float', 'int64', 'int32', 'int16',
                            'int8'])
    def unique_values(arr):
        return array_api.unique_values(arr)


@pytest.mark.xfail
@pytest.mark.parametrize('col', ('f8', 'f4', 'i8', 'i4', 'i2', 'i1'))
def test_unique_values(heavydb, col):

    table = heavydb.table_name + 'arraynullrepeat'
    query = f"SELECT {col}, unique_values({col}) FROM {table}"
    _, r = heavydb.sql_execute(query)
    for val, got in r:
        if val is None or val == []:
            assert got is None
        else:
            s = set(val)
            assert len(s) == len(got)
            for e in got:
                assert e in s
