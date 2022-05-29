import os
import pytest
from rbc.tests import heavydb_fixture


@pytest.fixture(scope="module")
def heavydb():

    for o in heavydb_fixture(globals(), minimal_version=(6, 1)):
        define(o)
        o.base_name = os.path.splitext(os.path.basename(__file__))[0]
        yield o


def define(heavydb):
    pass


@pytest.mark.parametrize("colname", ['f8', 'f4', 'i1', 'i2', 'i4', 'i8', 'b'])
def test_sum_along_row(heavydb, colname):

    _, result = heavydb.sql_execute(f'SELECT {colname} FROM {heavydb.table_name}array')
    mysum = any if colname == 'b' else sum
    expected_result = [(mysum(item[0]),) for item in result]

    _, result = heavydb.sql_execute('SELECT * FROM table(sum_along_row(CURSOR('
                                    f'SELECT {colname} FROM {heavydb.table_name}array)))')
    result = list(result)
    assert result == expected_result


@pytest.mark.parametrize("colname", ['f8', 'f4', 'i1', 'i2', 'i4', 'i8', 'b'])
def test_array_copier(heavydb, colname):

    _, result = heavydb.sql_execute(f'SELECT {colname} FROM {heavydb.table_name}array')
    expected_result = list(result)

    _, result = heavydb.sql_execute('SELECT * FROM table(array_copier(CURSOR('
                                    f'SELECT {colname} FROM {heavydb.table_name}array)))')
    result = list(result)
    assert result == expected_result


@pytest.mark.parametrize("colname", ['f8', 'f4', 'i1', 'i2', 'i4', 'i8', 'b'])
def test_array_concat(heavydb, colname):

    for n in [1, 3]:
        _, result = heavydb.sql_execute(f'SELECT {colname} FROM {heavydb.table_name}array')
        expected_result = [(item[0] * n,) for item in result]

        colnames = ", ".join([colname] * n)
        _, result = heavydb.sql_execute('SELECT * FROM table(array_concat(CURSOR('
                                        f'SELECT {colnames} FROM {heavydb.table_name}array)))')
        result = list(result)
        assert result == expected_result
