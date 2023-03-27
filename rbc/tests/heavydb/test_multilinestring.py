import pytest
import numpy as np
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 4),
                             suffices=['multilinestring']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<Z>, OutputColumn<K>)",
             Z=['GeoMultiLineString'], K=['GeoPolygon'], devices=['cpu'])
    def rbc_ct_to_polygon(mgr, mlinestrings, polygons):
        size = len(mlinestrings)
        mgr.set_output_item_values_total_number(0, mlinestrings.get_n_of_values())
        mgr.set_output_row_size(size)
        # Initialize polygons
        for i in range(size):
            if mlinestrings.is_null(i):
                polygons.set_null(i)
            else:
                coords = mlinestrings[i].toCoords()
                polygons[i].fromCoords(coords)
        return size


@pytest.mark.parametrize('col', ('ml1', 'ml2', 'ml3', 'ml4'))
def test_ct_to_polygon(heavydb, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    suffix = 'multilinestring'
    func = 'ct_to_polygon'

    query = (f'select * from table(rbc_{func}(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table({func}(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


