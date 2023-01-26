import pytest
import numpy as np
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 4),
                             suffices=['linestring']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<Z>, OutputColumn<Z>)",
             Z=['GeoLineString'], devices=['cpu'])
    def rbc_ct_copy(mgr, linestrings, copied_linestrings):
        size = len(linestrings)
        mgr.set_output_item_values_total_number(0, linestrings.getNofValues())
        mgr.set_output_row_size(size)
        for i in range(size):
            if linestrings.is_null(i):
                copied_linestrings.set_null(i)
            else:
                val = linestrings.get_item(i)
                copied_linestrings.set_null(i)
                # copied_linestrings.set_item(i, linestrings.get_item(i))
                # copied_linestrings[i] = linestrings[i]
        return size

    heavydb.register()


@pytest.mark.parametrize('col', ('l1', 'l2', 'l3', 'l4'))
def test_ct_coords(heavydb, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    suffix = 'linestring'
    query = (f'select * from table(rbc_ct_copy(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_copy(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
