import pytest
import numpy as np
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 4),
                             suffices=['linestring', 'multilinestring',
                                       'multipoint', 'polygon',
                                       'multipolygon']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<Z>, OutputColumn<Z>)",
             Z=['GeoLineString', 'GeoMultiLineString', 'GeoMultiPoint',
                'GeoPolygon', 'GeoMultiPolygon'],
             devices=['cpu'])
    def rbc_ct_copy(mgr, inputs, outputs):
        size = len(inputs)
        mgr.set_output_item_values_total_number(0, inputs.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if inputs.is_null(i):
                outputs.set_null(i)
            else:
                outputs[i] = inputs[i]
        return size

    heavydb.register()


@pytest.mark.parametrize('suffix,col', [
    ('linestring', 'l1'),
    ('linestring', 'l2'),
    ('linestring', 'l3'),
    ('linestring', 'l4'),
    ('multilinestring', 'ml1'),
    ('multilinestring', 'ml2'),
    ('multilinestring', 'ml3'),
    ('multilinestring', 'ml4'),
    ('multipoint', 'mp1'),
    ('multipoint', 'mp2'),
    ('multipoint', 'mp3'),
    ('multipoint', 'mp4'),
    ('polygon', 'p1'),
    ('polygon', 'p2'),
    ('polygon', 'p3'),
    ('polygon', 'p4'),
    ('multipolygon', 'mp1'),
    ('multipolygon', 'mp2'),
    ('multipolygon', 'mp3'),
    ('multipolygon', 'mp4'),
])
def test_ct_coords(heavydb, suffix, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    query = (f'select * from table(rbc_ct_copy(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_copy(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
