import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 6)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('int64(T[], int64)', T=['int8', 'int16', 'int32', 'int64', 'float', 'double'])
    def array_null_check(x, index):
        if x.is_null():  # array row is null
            return 0
        if x.is_null(index):  # array index is null
            return 1
        return 2


# skipping bool test since NULL is converted to true - rbc issue #245
colnames = ['i1', 'i2', 'i4', 'i8', 'f4', 'f8']


@pytest.mark.parametrize('col', colnames)
def test_array_isnull_issue240(omnisci, col):
    omnisci.require_version((5, 6),
                            'Requires omniscidb-internal PR 5104 [rbc issue 240]')

    _, result = omnisci.sql_execute(
        f'SELECT {col}, array_null_check({col}, 0) FROM {omnisci.table_name}array_null;')

    x, y = list(result)
    if x is None:
        assert y == 0
    else:
        assert y[1] == 1
