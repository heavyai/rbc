import pytest
import numpy as np
from rbc.tests import heavydb_fixture, _TestTable


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(7, 0),
                             suffices=['linestring', 'multilinestring',
                                       'multipoint',
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

    @heavydb("int32(TableFunctionManager, Cursor<T, T, T>, OutputColumn<Z>, OutputColumn<int>)",
             T=['GeoLineString'], Z=['GeoPolygon'], devices=['cpu'])
    def rbc_ct_make_polygon3(mgr, rings, holes1, holes2, polygons, sizes):
        size = len(rings)
        mgr.set_output_item_values_total_number(0,
                                                rings.get_n_of_values() +
                                                holes1.get_n_of_values() +
                                                holes2.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if rings.is_null(i):
                polygons.set_null(i)
                sizes.set_null(i)
            else:
                polygon_coords = []
                polygon_coords.append(rings[i].to_coords())
                if not holes1.is_null(i):
                    polygon_coords.append(holes1[i].to_coords())
                    if not holes2.is_null(i):
                        polygon_coords.append(holes2[i].to_coords())

                polygon = polygons[i]
                polygon.from_coords(polygon_coords)

                n_of_points = 0
                for j in range(len(polygon)):
                    n_of_points += len(polygon[j])
                sizes[i] = n_of_points
        return size


class _GeoPolygonTest(_TestTable):
    @property
    def suffix(self):
        return 'geo_polygon_test'

    @property
    def sqltypes(self):
        return ('LINESTRING',
                'LINESTRING',
                'LINESTRING',
                'GEOMETRY(LINESTRING, 4326)',
                'GEOMETRY(LINESTRING, 4326)',
                'GEOMETRY(LINESTRING, 4326)',
                'GEOMETRY(LINESTRING, 4326) ENCODING NONE',
                'GEOMETRY(LINESTRING, 4326) ENCODING NONE',
                'GEOMETRY(LINESTRING, 4326) ENCODING NONE',
                'GEOMETRY(LINESTRING, 900913)',
                'GEOMETRY(LINESTRING, 900913)',
                'GEOMETRY(LINESTRING, 900913)')

    @property
    def values(self):
        return {
            'r1': ['LINESTRING(1 2,3 4,5 6,7 8,9 10)', 'LINESTRING(0 0,5 0,5 5,0 5)',
                   'LINESTRING(1 2,3 4,5 6,7 8,9 10)', None],
            'h1': ['LINESTRING(2 3,3 4,1 2)', None, 'LINESTRING(2 3,3 4,1 2)', None],
            'hh1': [None, None, 'LINESTRING(9 10,5 6,7 8)', None],
            'r2': ['LINESTRING(0 0,5 0,5 5,0 5)', 'LINESTRING(0 0,7 0,7 7,0 7)',
                   'LINESTRING(0 0,5 0,5 5,0 5)', None],
            'h2': ['LINESTRING(2 2,2 1,1 1,1 2)', None,
                   'LINESTRING(2 2,2 1,1 1,1 2)', None],
            'hh2': [None, None, 'LINESTRING(0 0,0 1,1 0)', None],
            'r3': ['LINESTRING(0 0,6 0,6 6,0 6))', 'LINESTRING(0 0,7 0,7 7,0 7)',
                   'LINESTRING(0 0,6 0,6 6,0 6))', None],
            'h3': ['LINESTRING(3 3,3 2,2 2,2 3)', None,
                   'LINESTRING(3 3,3 2,2 2,2 3)', None],
            'hh3': [None, None, 'LINESTRING(0 0,0 1,1 0)', None],
            'r4': ['LINESTRING(0 0,7 0,7 7,0 7)', 'LINESTRING(0 0,4 0,4 4,0 4)',
                   'LINESTRING(0 0,7 0,7 7,0 7)', None],
            'h4': ['LINESTRING(4 4,4 2,2 3,2 4)', None,
                   'LINESTRING(4 4,4 2,2 3,2 4)', None],
            'hh4': [None, None, 'LINESTRING(0 0,0 1,1 0)', None],
        }


@pytest.fixture(scope='module')
def create_table(heavydb):
    table_name = 'geo_polygon_test'
    geo = _GeoPolygonTest()

    heavydb.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    heavydb.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({geo.table_defn})')
    heavydb.load_table_columnar(f'{table_name}', **geo.values)
    yield heavydb
    heavydb.sql_execute(f'DROP TABLE IF EXISTS {table_name}')


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
def test_ct_copy(heavydb, suffix, col):
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
    pytest.param('multipoint', 'mp2', marks=pytest.mark.xfail(reason='odd bug')),
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


@pytest.mark.usefixtures('create_table')
@pytest.mark.parametrize('rcol, hcol, hhcol', [
    ('r1', 'h1', 'hh1'),
    ('r2', 'h2', 'hh2'),
    ('r3', 'h3', 'hh3'),
    ('r4', 'h4', 'hh4'),
])
def test_ct_make_polygon3(heavydb, rcol, hcol, hhcol):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    table = 'geo_polygon_test'
    func = 'ct_make_polygon3'

    query = (f'select * from table(rbc_{func}(cursor(select {rcol}, {hcol}, {hhcol} '
             f'from {table})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table({func}(cursor(select {rcol}, {hcol}, {hhcol} '
             f'from {table})));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
