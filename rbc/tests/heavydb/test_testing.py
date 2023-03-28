import pytest
from rbc.tests import heavydb_fixture, test_suffices


@pytest.mark.parametrize('suffix', test_suffices)
def test_table_load(suffix):

    count = 0
    for heavydb in heavydb_fixture(globals(), suffices=[suffix]):
        heavydb.require_version((5, 7, 0),
                                'Requires heavydb-internal PR 5465 [rbc PR 330]')
        if suffix == 'arraynull':
            heavydb.require_version((5, 7, 0),
                                    'Requires heavydb-internal PR 5492 [rbc issue 245]')
        if suffix in ('multipoint', 'multilinestring'):
            heavydb.require_version((6, 2, 0),
                                    'Requires HeavyDB 6.2 or newer')
        count += 1
        descr, result = heavydb.sql_execute(f'select * from {heavydb.table_name}{suffix}')
        result = list(result)
        colnames = [d.name for d in descr]

    assert count == 1

    if suffix == '':
        assert colnames == ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
        assert result == [(0.0, 0.0, 0, 0, 0, 0, 1),
                          (1.0, 1.0, 1, 1, 1, 1, 0),
                          (2.0, 2.0, 2, 2, 2, 2, 1),
                          (3.0, 3.0, 3, 3, 3, 3, 0),
                          (4.0, 4.0, 4, 4, 4, 4, 1)]
    elif suffix == '10':
        assert colnames == ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
        assert result == [(0.0, 0.0, 0, 0, 0, 0, 1),
                          (1.0, 1.0, 1, 1, 1, 1, 0),
                          (2.0, 2.0, 2, 2, 2, 2, 1),
                          (3.0, 3.0, 3, 3, 3, 3, 0),
                          (4.0, 4.0, 4, 4, 4, 4, 1),
                          (5.0, 5.0, 5, 5, 5, 5, 0),
                          (6.0, 6.0, 6, 6, 6, 6, 1),
                          (7.0, 7.0, 7, 7, 7, 7, 0),
                          (8.0, 8.0, 8, 8, 8, 8, 1),
                          (9.0, 9.0, 9, 9, 9, 9, 0)]
    elif suffix == 'null':
        assert colnames == ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
        assert result == [(None, 0.0, 0, None, 0, 0, None),
                          (1.0, 1.0, None, 1, 1, None, 0),
                          (2.0, None, 2, 2, None, 2, 1),
                          (None, 3.0, 3, None, 3, 3, None),
                          (4.0, 4.0, None, 4, 4, None, 1)]
    elif suffix == 'array':
        assert colnames == ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
        assert result == [([], [], [], [], [], [], []),
                          ([1.0], [1.0], [1], [1], [1], [1], [0]),
                          ([2.0, 3.0], [2.0, 3.0], [2, 3], [2, 3], [2, 3], [2, 3], [1, 0]),
                          ([3.0, 4.0, 5.0], [3.0, 4.0, 5.0], [3, 4, 5], [3, 4, 5],
                           [3, 4, 5], [3, 4, 5], [0, 1, 0]),
                          ([4.0, 5.0, 6.0, 7.0], [4.0, 5.0, 6.0, 7.0], [4, 5, 6, 7],
                           [4, 5, 6, 7], [4, 5, 6, 7], [4, 5, 6, 7], [1, 0, 1, 0])]
    elif suffix == 'arraynull':
        assert colnames == ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
        assert result == [(None, [], None, [], None, [], None),
                          ([1.0], None, [None], None, [1], None, [0]),
                          (None, [None, 3.0], None, [2, None], None, [2, 3], None),
                          ([None, 4.0, 5.0], None, [3, None, 5], None, [3, 4, None],
                           None, [None, 1, 0]),
                          (None, [4.0, None, 6.0, 7.0], None, [4, 5, None, 7], None,
                           [None, 5, 6, None], None)]
    elif suffix == 'text':
        assert colnames == ['t4', 't2', 't1', 's', 'n', 'n2']
        assert result == [('foofoo', 'foofoo', 'fun', ['foo', 'bar'], 'fun', '1'),
                          ('bar', 'bar', 'bar', ['fun', 'bar'], 'bar', '12'),
                          ('fun', 'fun', 'foo', ['foo'], 'foo', '123'),
                          ('bar', 'bar', 'barr', ['foo', 'bar'], 'barr', '1234'),
                          ('foo', 'foo', 'foooo', ['fun', 'bar'], 'foooo', '12345')]
    elif suffix == 'timestamp':
        assert colnames == ['t9', 't9_2', 't9_null', 'i8_2', 't6']
        assert result == [
            (31539661001001001, 1609462861001001001, 65844122002002002, 1609462861001001001, 31539661001001),  # noqa: E501
            (65844122002002002, 1643767322002002002, None, 1643767322002002002, 65844122002002),
            (99975783003003003, 1677812583003003003, 2117152922002002002, 1677812583003003003, 99975783003003)  # noqa: E501
        ]
    elif suffix == 'geopoint':
        assert colnames == ['p1', 'p2', 'p3', 'p4']
        assert result == [
            ('POINT (1 2)', 'POINT (2.99999999022111 3.99999997299165)',
             'POINT (5 6)', 'POINT (7 8)'),
            ('POINT (9 8)', 'POINT (6.99999992130324 5.99999998044223)',
             'POINT (5 4)', 'POINT (3 2)'),
            (None, None, None, None)]
    elif suffix == 'linestring':
        assert colnames == ['l1', 'l2', 'l3', 'l4']
        assert result == [
            ('LINESTRING (1 2,3 5)', 'LINESTRING (3 4,5 7)',
             'LINESTRING (5 6,7 9)', 'LINESTRING (7 8,9 11)'),
            ('LINESTRING (9 8,11 11)', 'LINESTRING (7 6,9 9)',
             'LINESTRING (5 4,7 7)', 'LINESTRING (3 2,5 5)'),
            (None, None, None, None)]
    elif suffix == 'multipoint':
        assert colnames == ['mp1', 'mp2', 'mp3', 'mp4']
        assert result == [
            ('MULTIPOINT (1 2,3 5)',
             'MULTIPOINT (2.99999999022111 3.99999997299165,4.99999995576218 6.99999996321276)',
             'MULTIPOINT (5 6,7 9)', 'MULTIPOINT (7 8,9 11)'),
            ('MULTIPOINT (9 8,11 11)',
             'MULTIPOINT (6.99999992130324 5.99999998044223,8.99999997066334 8.99999997066334)',
             'MULTIPOINT (5 4,7 7)', 'MULTIPOINT (3 2,5 5)'),
            (None, None, None, None)
        ]
    elif suffix == 'polygon':
        assert colnames == ['p1', 'p2', 'p3', 'p4']
        assert result == [
            ('POLYGON ((1 2,3 4,5 6,7 8,9 10,1 2),(1 2,3 4,2 3,1 2))',
                'POLYGON ((0 0,4.99999995576218 0.0,4.99999995576218 4.99999999767169,0.0 '
                '4.99999999767169,0 0),(1.99999996554106 1.99999996554106,1.99999996554106 '
                '0.999999982770532,0.999999940861017 0.999999982770532,0.999999940861017 '
                '1.99999996554106,1.99999996554106 1.99999996554106))',
                'POLYGON ((0 0,7 0,7 7,0 7,0 0),(4 4,4 2,2 3,2 4,4 4))',
                'POLYGON ((0 0,6 0,6 6,0 6,0 0),(3 3,3 2,2 2,2 3,3 3))'),
            ('POLYGON ((0 0,5 0,5 5,0 5,0 0))',
                'POLYGON ((0 0,5.99999998044223 0.0,5.99999998044223 5.99999998044223,0.0 '
                '5.99999998044223,0 0))',
                'POLYGON ((0 0,7 0,7 7,0 7,0 0))',
                'POLYGON ((0 0,4 0,4 4,0 4,0 0))'),
            ('POLYGON ((1 2,3 4,5 6,7 8,9 10,1 2),(2 3,1 2,3 4,2 3),(9 10,7 8,5 6,9 10))',
                'POLYGON ((0 0,4.99999995576218 0.0,4.99999995576218 4.99999999767169,0.0 '
                '4.99999999767169,0 0),(1.99999996554106 1.99999996554106,1.99999996554106 '
                '0.999999982770532,0.999999940861017 0.999999982770532,0.999999940861017 '
                '1.99999996554106,1.99999996554106 1.99999996554106),(0 0,0.0 '
                '0.999999982770532,0.999999940861017 0.0,0 0))',
                'POLYGON ((0 0,6 0,6 6,0 6,0 0),(3 3,3 2,2 2,2 3,3 3),(0 0,0 1,1 0,0 0))',
                'POLYGON ((0 0,7 0,7 7,0 7,0 0),(4 4,4 2,2 3,2 4,4 4),(0 0,0 1,1 0,0 0))'),
            (None, None, None, None)
        ]
    elif suffix == 'multipolygon':
        assert colnames == ['mp1', 'mp2', 'mp3', 'mp4']
        assert result == [
            ('MULTIPOLYGON (((1 2,3 4,5 6,7 8,9 10,1 2),(1 2,3 4,2 3,1 2)))',
             'MULTIPOLYGON (((0 0,4.99999995576218 0.0,4.99999995576218 '
             '4.99999999767169,0.0 4.99999999767169,0 0),(1.99999996554106 '
             '1.99999996554106,1.99999996554106 0.999999982770532,0.999999940861017 '
             '0.999999982770532,0.999999940861017 1.99999996554106,1.99999996554106 '
             '1.99999996554106)))',
             'MULTIPOLYGON (((0 0,6 0,6 6,0 6,0 0),(3 3,3 2,2 2,2 3,3 3)))',
             'MULTIPOLYGON (((0 0,7 0,7 7,0 7,0 0),(4 4,4 2,2 3,2 4,4 4)))'),
            ('MULTIPOLYGON (((0 0,5 0,5 5,0 5,0 0)))',
             'MULTIPOLYGON (((0 0,5.99999998044223 0.0,5.99999998044223 '
             '5.99999998044223,0.0 5.99999998044223,0 0)))',
             'MULTIPOLYGON (((0 0,7 0,7 7,0 7,0 0)))',
             'MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)))'),
            ('MULTIPOLYGON (((1 2,3 4,5 6,7 8,9 10,1 2),(2 3,1 2,3 4,2 3),(9 10,7 8,5 6,9 '
             '10)))',
             'MULTIPOLYGON (((0 0,4.99999995576218 0.0,4.99999995576218 '
             '4.99999999767169,0.0 4.99999999767169,0 0),(1.99999996554106 '
             '1.99999996554106,1.99999996554106 0.999999982770532,0.999999940861017 '
             '0.999999982770532,0.999999940861017 1.99999996554106,1.99999996554106 '
             '1.99999996554106),(0 0,0.0 0.999999982770532,0.999999940861017 0.0,0 0)))',
             'MULTIPOLYGON (((0 0,6 0,6 6,0 6,0 0),(3 3,3 2,2 2,2 3,3 3),(0 0,0 1,1 0,0 '
             '0)))',
             'MULTIPOLYGON (((0 0,7 0,7 7,0 7,0 0),(4 4,4 2,2 3,2 4,4 4),(0 0,0 1,1 0,0 '
             '0)))'),
            (None, None, None, None)
        ]
    elif suffix == 'multilinestring':
        assert colnames == ['ml1', 'ml2', 'ml3', 'ml4']
        assert result == [
            ('MULTILINESTRING ((1 2,3 4,5 6,7 8,9 10),(2 3,3 4,1 2))',
             'MULTILINESTRING ((0 0,5 0,5 5,0 5),(2 2,2 1,1 1,1 2))',
             'MULTILINESTRING ((0 0,6 0,6 6,0 6),(3 3,3 2,2 2,2 3))',
             'MULTILINESTRING ((0 0,7 0,7 7,0 7),(4 4,2 4,2 3,4 2))'),
            ('MULTILINESTRING ((0 0,5 0,5 5,0 5))',
             'MULTILINESTRING ((0 0,6 0,6 6,0 6))',
             'MULTILINESTRING ((0 0,7 0,7 7,0 7))',
             'MULTILINESTRING ((0 0,4 0,4 4,0 4))'),
            ('MULTILINESTRING ((1 2,3 4,5 6,7 8,9 10),(3 4,1 2,2 3),(5 6,7 8,9 10))',
             'MULTILINESTRING ((0 0,5 0,5 5,0 5),(2 2,2 1,1 1,1 2),(0 0,0 1,1 0))',
             'MULTILINESTRING ((0 0,6 0,6 6,0 6),(3 3,3 2,2 2,2 3),(0 0,0 1,1 0))',
             'MULTILINESTRING ((0 0,7 0,7 7,0 7),(4 4,2 4,2 3,4 2),(0 0,0 1,1 0))'),
            (None, None, None, None),
        ]
    else:
        raise NotImplementedError(suffix)
