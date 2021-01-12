import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 5)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('int32(Column<int64>, RowMultiplier, OutputColumn<int64>)')
    def text_rbc_copy_c_rowmul(x, m, y):
        for i in range(len(x)):
            # TODO: implement is_null support
            y[i] = x[i]
        return len(x)

    @omnisci('int32(Cursor<int64, double>, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<double>)')
    def text_rbc_copy_cc_rowmul(x, x2, m, y, y2):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
        return len(x)

    @omnisci('int32(Cursor<int64>, Cursor<double>, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<double>)')
    def text_rbc_copy_c_c_rowmul(x, x2, m, y, y2):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
        return len(x)

    @omnisci('int32(Cursor<int32>, Cursor<double, int64>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<double>, OutputColumn<int64>)')
    def text_rbc_copy_c_cc_rowmul(x, x2, x3, m, y, y2, y3):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
        return len(x)

    @omnisci('int32(Cursor<int32, int64>, Cursor<double>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<int64>, OutputColumn<double>)')
    def text_rbc_copy_cc_c_rowmul(x, x2, x3, m, y, y2, y3):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
        return len(x)

    @omnisci('int32(Cursor<int32, int64>, Cursor<double, float>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<int64>, OutputColumn<double>,'
             ' OutputColumn<float>)')
    def text_rbc_copy_cc_cc_rowmul(x, x2, x3, x4, m, y, y2, y3, y4):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
            y4[i] = x4[i]
        return len(x)

    @omnisci('int32(Cursor<int32, int64, double, float>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<int64>, OutputColumn<double>,'
             ' OutputColumn<float>)')
    def text_rbc_copy_cccc_rowmul(x, x2, x3, x4, m, y, y2, y3, y4):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
            y4[i] = x4[i]
        return len(x)


@pytest.mark.parametrize("inputs",
                         ['i8', 'cursor(i8,f8)', 'i8;f8', 'i4;cursor(f8,i8)',
                          'cursor(i4,i8);f8', 'cursor(i4,i8);cursor(f8,f4)',
                          'cursor(i4,i8,f8,f4)'])
def test_copy(omnisci, inputs):
    omnisci.require_version((5, 5), 'Requires omniscidb-internal PR 5134')

    groups = inputs.split(';')
    table_names = [f'{omnisci.table_name}'] * len(groups)

    group_results = {}
    args = []
    cc = []
    expected = [()] * 5
    for colnames, table_name in zip(groups, table_names):
        if colnames.startswith('cursor('):
            colnames = colnames[7:-1]
        result = group_results.get(colnames)
        if result is None:
            _, result = omnisci.sql_execute(f'select {colnames} from {table_name}')
            result = group_results[colnames] = list(result)
        expected = [row1 + row2 for row1, row2 in zip(expected, result)]
        args.append(f'cursor(select {colnames} from {table_name})')
        cc.append((colnames.count(',') + 1) * 'c')
    args.append('1')
    args = ', '.join(args)
    cc = '_'.join(cc)

    query = f'select * from table(text_rbc_copy_{cc}_rowmul({args}))'
    _, result = omnisci.sql_execute(query)
    result = list(result)

    assert result == expected
