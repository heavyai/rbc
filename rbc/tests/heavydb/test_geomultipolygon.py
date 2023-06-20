import pytest
import numpy as np
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(7, 0),
                             suffices=['multipolygon']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<Z>, int64, OutputColumn<K>)",
             Z=['GeoMultiPolygon'], K=['GeoPolygon'], devices=['cpu'])
    def rbc_ct_polygonn(mgr, mpolygons, n, polygons):
        size = len(mpolygons)
        mgr.set_output_item_values_total_number(0, mpolygons.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if mpolygons.is_null(i):
                polygons.set_null(i)
            else:
                polygons.set_item(i, mpolygons[i][n - 1])
        return size


@pytest.mark.parametrize('n', ('1',))
@pytest.mark.parametrize('suffix,col', [
    ('multipolygon', 'mp1'),
    ('multipolygon', 'mp2'),
    ('multipolygon', 'mp3'),
    ('multipolygon', 'mp4'),
])
def test_ct_polygonn(heavydb, suffix, col, n):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    query = (f'select * from table(rbc_ct_polygonn(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), {n}));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_polygonn(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), {n}));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
