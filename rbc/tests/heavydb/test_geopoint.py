import pytest
import numpy as np
from rbc.heavydb import Point2D
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(7, 0),
                             suffices=['point']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<GeoPoint>, OutputColumn<Z>, OutputColumn<Z>)",
             Z=['double'], devices=['cpu'])
    def rbc_ct_coords(mgr, points, xcoords, ycoords):
        size = len(points)
        mgr.set_output_row_size(size)
        for i in range(size):
            if points.is_null(i):
                xcoords.set_null(i)
                ycoords.set_null(i)
            else:
                point = points[i]
                xcoords[i] = point.x
                ycoords[i] = point.y
        return size

    @heavydb("int32(TableFunctionManager, Column<T>, double, double, OutputColumn<T>)",
             T=['GeoPoint'], devices=['cpu'])
    def rbc_ct_shift(mgr, points, x, y, shifted_points):
        size = len(points)
        mgr.set_output_row_size(size)
        for i in range(size):
            if points.is_null(i):
                shifted_points.set_null(i)
            else:
                point = points[i]
                shifted_points.set_item(i, Point2D(point.x + x, point.y + y))
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
                line = [x[i], y[i], x[i] + dx, y[i] + dy]
                linestrings[i].from_coords(line)
        return size


@pytest.mark.parametrize('suffix', ('point',))
@pytest.mark.parametrize('col', ('p1', 'p2', 'p3', 'p4'))
def test_ct_coords(heavydb, suffix, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    query = (f'select * from table(rbc_ct_coords(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_coords(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('suffix', ('point',))
@pytest.mark.parametrize('col', ('p1', 'p2', 'p3', 'p4'))
def test_ct_shift(heavydb, suffix, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    query = (f'select * from table(rbc_ct_shift(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), 1.5, -2.5));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_shift(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), 1.5, -2.5));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('col', ('p1', 'p2', 'p3', 'p4'))
@pytest.mark.parametrize('dx,dy', [('2', '3')])
def test_ct_make_linestring2(heavydb, col, dx, dy):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    suffix = 'point'
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
