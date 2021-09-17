import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 5)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('int32(Column<T>, RowMultiplier, OutputColumn<T>)',
             T=['int8', 'int16', 'int32', 'int64', 'float32', 'float64'])
    def my_row_copier_mul(x, m, y):
        input_row_count = len(x)
        for i in range(input_row_count):
            if x.is_null(i):
                y.set_null(i)
            else:
                y[i] = x[i] + x[i]
        return m * input_row_count

    # cannot overload boolean8 and int8 as boolean8 is interpreted as int8
    @omnisci('int32(Column<bool>, RowMultiplier, OutputColumn<bool>)')
    def my_row_copier_mul_bool(x, m, y):
        input_row_count = len(x)
        for i in range(input_row_count):
            if x.is_null(i):
                y.set_null(i)
            elif x[i]:
                y[i] = False
            else:
                y[i] = True
        return m * input_row_count


colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']


@pytest.mark.parametrize('col', colnames)
def test_null_value(omnisci, col):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 188]')

    typ = dict(f4='float32', f8='float64', i1='int8', i2='int16',
               i4='int32', i8='int64', b='bool')[col]

    if typ == 'bool':
        prefix = '_bool'
        descr, expected = omnisci.sql_execute(
            f'select {col}, not {col} from {omnisci.table_name}null')
        data, expected = zip(*list(expected))

    else:
        prefix = ''
        descr, expected = omnisci.sql_execute(
            f'select {col}, {col} + {col} from {omnisci.table_name}null')
        data, expected = zip(*list(expected))

    descr, result = omnisci.sql_execute(
        f'select * from table(my_row_copier_mul{prefix}(cursor(select {col} '
        f'from {omnisci.table_name}null), 1));')
    result, = zip(*list(result))

    assert result == expected, (result, expected, data)


def test_row_adder(omnisci):
    omnisci.require_version((5, 5),
                            'Requires omniscidb-internal PR 5104 [rbc issue 188]')

    descr, expected = omnisci.sql_execute(
        f'select f8, f8 + f8 from {omnisci.table_name}null')
    data, expected = zip(*list(expected))

    descr, result = omnisci.sql_execute(
        'select * from table(row_adder(1, cursor(select f8, f8 '
        f'from {omnisci.table_name}null)))')
    result, = zip(*list(result))

    assert result == expected, (result, expected, data)
