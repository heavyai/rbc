import pytest
from rbc.tests import omnisci_fixture

ARRAY_NULL = 0
ARRAY_IDX_IS_NULL = 1
ARRAY_NOT_NULL = 2


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 5)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('int64(T[], int64)', T=['bool', 'int8', 'int16', 'int32', 'int64', 'float', 'double'])
    def array_null_check(x, index):
        if x.is_null():  # array row is null
            return ARRAY_NULL
        if x.is_null(index):  # array index is null
            return ARRAY_IDX_IS_NULL
        return ARRAY_NOT_NULL


colnames = ['b', 'i1', 'i2', 'i4', 'i8', 'f4', 'f8']


@pytest.mark.parametrize('col', colnames)
def test_array_null(omnisci, col):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 240]')

    # skipping bool test since NULL is converted to true - rbc issue #245
    if col == 'b':
        pytest.skip('Skipping Array<boolean> test [RBC issue 240]')

    # Query null value
    _, result = omnisci.sql_execute(f'''
        SELECT
            array_null_check({col}, 0)
        FROM
            {omnisci.table_name}arraynull
    ''')

    expected = {
        'i1': [(0,), (1,), (0,), (2,), (0,)],
        'i2': [(2,), (0,), (2,), (0,), (2,)],
        'i4': [(0,), (2,), (0,), (2,), (0,)],
        'i8': [(2,), (0,), (2,), (0,), (1,)],
        'f4': [(0,), (2,), (0,), (1,), (0,)],
        'f8': [(1,), (0,), (1,), (0,), (2,)],
    }

    assert list(result) == expected[col]
