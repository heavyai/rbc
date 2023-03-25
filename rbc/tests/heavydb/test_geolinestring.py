import pytest
import numpy as np
from rbc.heavydb import Array
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 4),
                             suffices=['linestring', 'multilinestring',
                                       'geopoint', 'multipoint',
                                       'polygon', 'multipolygon']):
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

    @heavydb("int32(TableFunctionManager, Column<T>, int64, OutputColumn<Z>, OutputColumn<Z>)",
             T=['GeoMultiPoint', 'GeoLineString'], Z=['double'], devices=['cpu'])
    def rbc_ct_pointn(mgr, points, n, xcoords, ycoords):
        size = len(points)
        mgr.set_output_row_size(size)
        for i in range(size):
            if points.is_null(i):
                xcoords.set_null(i)
                ycoords.set_null(i)
            else:
                point = points[i][n - 1]  # n is one-based
                xcoords[i] = point.x
                ycoords[i] = point.y
        return size

    @heavydb("int32(TableFunctionManager, Cursor<Column<T>, Column<T>>, T, T, OutputColumn<Z>)",
             T=['double'], Z=['GeoLineString'], devices=['cpu'])
    def rbc_ct_make_linestring2(mgr, x, y, dx, dy, linestrings):
        size = len(x)
        mgr.set_output_item_values_total_number(0, size * 4)
        mgr.set_output_row_size(size)
        for i in range(size):
            if x.is_null(i) or y.is_null(i):
                linestrings.set_null(i)
            else:
                line = Array([x[i], y[i], x[i] + dx, y[i] + dy])
                linestrings[i].fromCoords(line)
        return size


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


@pytest.mark.parametrize('n', ('1',))
@pytest.mark.parametrize('suffix,col', [
    ('linestring', 'l1'),
    ('linestring', 'l2'),
    ('linestring', 'l3'),
    ('linestring', 'l4'),
    ('multipoint', 'mp1'),
    ('multipoint', 'mp2'),
    ('multipoint', 'mp3'),
    ('multipoint', 'mp4'),
])
def test_ct_pointn(heavydb, suffix, col, n):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    query = (f'select * from table(rbc_ct_pointn(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), {n}));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_pointn(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), {n}));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('col', ('p1', 'p2', 'p3', 'p4'))
@pytest.mark.parametrize('dx,dy', [('2', '3')])
def test_ct_make_linestring2(heavydb, col, dx, dy):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    suffix = 'geopoint'
    table = heavydb.table_name + suffix

    query = ("SELECT * FROM TABLE(rbc_ct_make_linestring2(CURSOR(SELECT "
             f"ST_X({col}), ST_Y({col}) FROM {table}), {dx}, {dy}));")
    _, result = heavydb.sql_execute(query)

    query = (f"SELECT * FROM TABLE(ct_make_linestring2(CURSOR(SELECT "
             f"ST_X({col}), ST_Y({col}) FROM {table}), {dx}, {dy}));")
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
