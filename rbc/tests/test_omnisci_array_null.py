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

    import rbc.omnisci_backend as omni

    @omnisci('T[](T[], int64)', T=['int8', 'int16', 'int32', 'int64', 'float', 'double'])
    def set_null_value(x, index):
        z = omni.array(x)
        z.set_null(index)
        return z

    @omnisci('int64(T[], int64)', T=['int8', 'int16', 'int32', 'int64', 'float', 'double'])
    def array_null_check(x, index):
        if x.is_null():  # array row is null
            return ARRAY_NULL
        if x.is_null(index):  # array index is null
            return ARRAY_IDX_IS_NULL
        return ARRAY_NOT_NULL


# skipping bool test since NULL is converted to true - rbc issue #245
colnames = ['i1', 'i2', 'i4', 'i8', 'f4', 'f8']


@pytest.mark.parametrize('col', colnames)
def test_array_is_null(omnisci, col):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 240]')

    # Query null value
    _, result = omnisci.sql_execute(
        f'SELECT array_null_check({col}, -1) FROM {omnisci.table_name}array_null '
        f'WHERE {col} IS NULL')

    assert list(result)[0] == (ARRAY_NULL,)  # array is null


@pytest.mark.parametrize('col', colnames)
def test_array_is_not_null(omnisci, col):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 240]')

    # Query null value
    _, result = omnisci.sql_execute(
        f'SELECT array_null_check({col}, 1) FROM {omnisci.table_name}array_null '
        f'WHERE {col} IS NOT NULL')

    assert list(result)[0] == (ARRAY_NOT_NULL,)  # array is null


@pytest.mark.parametrize('col', colnames)
def test_array_idx_is_null(omnisci, col):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 240]')

    # Use a new table called "array_null_temp"
    omnisci.sql_execute('DROP TABLE IF EXISTS array_null_temp')

    # set null values - We can't rely on omniscidb setting null values at the
    # moment due to a bug where it wrongly uses scalar null values inside arrays.
    # See rbc issue #254. We set the correct null values first and later query
    # those values.
    _, result = omnisci.sql_execute(
        'CREATE TABLE array_null_temp AS '
        f'SELECT set_null_value({col}, 0) AS key FROM {omnisci.table_name}array_null '
        f'where {col} is not NULL')

    # Query null value
    _, result = omnisci.sql_execute(
        'SELECT array_null_check(key, 0) FROM array_null_temp')

    assert list(result)[0] == (ARRAY_IDX_IS_NULL,)  # key[index] is null


@pytest.mark.parametrize('col', colnames)
def test_array_idx_not_null(omnisci, col):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 240]')

    # Query null value
    _, result = omnisci.sql_execute(
        f'SELECT array_null_check({col}, 1) FROM {omnisci.table_name}array_null '
        f'WHERE {col} IS NOT NULL')

    assert list(result)[0] == (ARRAY_NOT_NULL,)  # array[index] is not null
