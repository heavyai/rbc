import pytest
from rbc.tests import heavydb_fixture


@pytest.mark.parametrize('mth', ['columnar', 'query'])
@pytest.mark.parametrize('suffix', ['', '10', 'null', 'array', 'arraynull'])
def test_table_load(mth, suffix):

    load_columnar = mth == 'columnar'

    count = 0
    for heavydb in heavydb_fixture(globals(), load_columnar=load_columnar):
        heavydb.require_version((5, 7, 0),
                                'Requires heavydb-internal PR 5465 [rbc PR 330]')
        if load_columnar and suffix == 'arraynull':
            heavydb.require_version((5, 7, 0),
                                    'Requires heavydb-internal PR 5492 [rbc issue 245]')
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
    else:
        raise NotImplementedError(suffix)
