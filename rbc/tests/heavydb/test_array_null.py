import pytest
from rbc.tests import heavydb_fixture

ARRAY_NULL = 0
ARRAY_IDX_IS_NULL = 1
ARRAY_NOT_NULL = 2


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(5, 6)):
        define(o)
        yield o


def define(heavydb):

    @heavydb('int64(T[], int64)', T=['bool', 'int8', 'int16', 'int32', 'int64', 'float', 'double'])
    def array_null_check(x, index):
        if x.is_null():  # array row is null
            return ARRAY_NULL
        if x.is_null(index):  # array index is null
            return ARRAY_IDX_IS_NULL
        return ARRAY_NOT_NULL


colnames = ['b', 'i1', 'i2', 'i4', 'i8', 'f4', 'f8']


@pytest.mark.parametrize('col', colnames)
def test_array_null(heavydb, col):
    if col in ['i2', 'i8', 'f8']:
        heavydb.require_version((5, 7, 0),
                                'Requires heavydb-internal PR 5465 [rbc PR 330]')
    if col == 'b':
        heavydb.require_version((5, 7, 0),
                                'Requires heavydb-internal PR 5492 [rbc issue 245]')

    # Query null value
    _, result = heavydb.sql_execute(f'''
        SELECT
            array_null_check({col}, 0)
        FROM
            {heavydb.table_name}arraynull
    ''')
    result = list(result)

    expected = {
        'b':  [(ARRAY_NULL,), (ARRAY_NOT_NULL,), (ARRAY_NULL,),
               (ARRAY_IDX_IS_NULL,), (ARRAY_NULL,)],
        'i1': [(ARRAY_NULL,), (ARRAY_IDX_IS_NULL,), (ARRAY_NULL,),
               (ARRAY_NOT_NULL,), (ARRAY_NULL,)],
        'i2': [(ARRAY_NOT_NULL,), (ARRAY_NULL,), (ARRAY_NOT_NULL,),
               (ARRAY_NULL,), (ARRAY_NOT_NULL,)],
        'i4': [(ARRAY_NULL,), (ARRAY_NOT_NULL,), (ARRAY_NULL,),
               (ARRAY_NOT_NULL,), (ARRAY_NULL,)],
        'i8': [(ARRAY_NOT_NULL,), (ARRAY_NULL,), (ARRAY_NOT_NULL,),
               (ARRAY_NULL,), (ARRAY_IDX_IS_NULL,)],
        'f4': [(ARRAY_NULL,), (ARRAY_NOT_NULL,), (ARRAY_NULL,),
               (ARRAY_IDX_IS_NULL,), (ARRAY_NULL,)],
        'f8': [(ARRAY_IDX_IS_NULL,), (ARRAY_NULL,), (ARRAY_IDX_IS_NULL,),
               (ARRAY_NULL,), (ARRAY_NOT_NULL,)],
    }

    assert result == expected[col]
