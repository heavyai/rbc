import pytest
import numpy as np
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 4),
                             suffices=['polygon']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<Z>, int64_t, OutputColumn<K>)",
             Z=['GeoPolygon'], K=['GeoLineString'], devices=['cpu'])
    def rbc_ct_linestringn(mgr, polygons, n, linestrings):
        size = len(polygons)
        mgr.set_output_item_values_total_number(0, polygons.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if polygons.is_null(i):
                linestrings.set_null(i)
            else:
                sz = len(polygons[i])
                if n < 1 or n > sz:
                    linestrings.set_null(i)
                else:
                    poly = polygons[i]
                    ring = poly[n - 1]
                    linestrings[i] = ring
        return size

    @heavydb("int32(TableFunctionManager, Column<Z>, OutputColumn<K>)",
             Z=['GeoPolygon'], K=['GeoMultiLineString'], devices=['cpu'])
    def rbc_ct_to_multilinestring(mgr, polygons, mlinestrings):
        size = len(polygons)
        mgr.set_output_item_values_total_number(0, polygons.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if polygons.is_null(i):
                mlinestrings.set_null(i)
            else:
                mlinestrings.set_null(i)
                polygon_coords = polygons[i].toCoords()
                # mlinestrings[i].fromCoords(polygon_coords)
        return size




# EXTENSION_NOINLINE int32_t
# ct_to_multilinestring__cpu_(TableFunctionManager& mgr,
#                             const Column<GeoPolygon>& polygons,
#                             Column<GeoMultiLineString>& mlinestrings) {
#   auto size = polygons.size();
#   mgr.set_output_item_values_total_number(0, polygons.getNofValues());
#   mgr.set_output_row_size(size);
#   // Initialize mlinestrings
#   int count_nulls = 0;
#   FlatBufferManager::Status status{};
#   for (int64_t i = 0; i < size; i++) {
#     if (polygons.isNull(i)) {
#       mlinestrings.setNull(i);
#       count_nulls++;
#     } else {
#       std::vector<std::vector<double>> polygon_coords = polygons[i].toCoords();
#       status = mlinestrings[i].fromCoords(polygon_coords);
#       if (status != FlatBufferManager::Status::Success) {
#         return mgr.ERROR_MESSAGE("fromCoords failed: " + ::toString(status));
#       }
#     }
#   }
#   return size;
# }

@pytest.mark.parametrize('n', ('1', '2', '3'))
@pytest.mark.parametrize('col', ('p1', 'p2', 'p3', 'p4'))
def test_ct_coords(heavydb, n, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    suffix = 'polygon'

    query = (f'select * from table(rbc_ct_linestringn(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), {n}));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_linestringn(cursor(select {col} '
             f'from {heavydb.table_name}{suffix}), {n}));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('col', ('p1', 'p2', 'p3', 'p4'))
def test_ct_to_multilinestring(heavydb, col):
    if heavydb.version[:2] < (6, 4):
        pytest.skip('Requires HeavyDB 6.4 or newer')

    suffix = 'polygon'

    query = (f'select * from table(rbc_ct_to_multilinestring(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select * from table(ct_to_multilinestring(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
