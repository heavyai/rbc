import pytest
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():

    for o in heavydb_fixture(globals(), minimal_version=(5, 6)):
        define(o)
        yield o


def define(heavydb):

    @heavydb('int32(Column<int64>, RowMultiplier, OutputColumn<int64>)')
    def text_rbc_copy_c_rowmul(x, m, y):
        for i in range(len(x)):
            # TODO: implement is_null support
            y[i] = x[i]
        return len(x)

    @heavydb('int32(Cursor<int64, double>, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<double>)')
    def text_rbc_copy_cc_rowmul(x, x2, m, y, y2):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
        return len(x)

    @heavydb('int32(Cursor<int64>, Cursor<double>, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<double>)')
    def text_rbc_copy_c_c_rowmul(x, x2, m, y, y2):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
        return len(x)

    @heavydb('int32(Cursor<int32>, Cursor<double, int64>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<double>, OutputColumn<int64>)')
    def text_rbc_copy_c_cc_rowmul(x, x2, x3, m, y, y2, y3):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
        return len(x)

    @heavydb('int32(Cursor<int32, int64>, Cursor<double>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<int64>, OutputColumn<double>)')
    def text_rbc_copy_cc_c_rowmul(x, x2, x3, m, y, y2, y3):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
        return len(x)

    @heavydb('int32(Cursor<int32, int64>, Cursor<double, float>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<int64>, OutputColumn<double>,'
             ' OutputColumn<float>)')
    def text_rbc_copy_cc_cc_rowmul(x, x2, x3, x4, m, y, y2, y3, y4):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
            y4[i] = x4[i]
        return len(x)

    @heavydb('int32(Cursor<int32, int64, double, float>, RowMultiplier,'
             ' OutputColumn<int32>, OutputColumn<int64>, OutputColumn<double>,'
             ' OutputColumn<float>)')
    def text_rbc_copy_cccc_rowmul(x, x2, x3, x4, m, y, y2, y3, y4):
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
            y4[i] = x4[i]
        return len(x)


@pytest.mark.parametrize('use_default', [True, False])
@pytest.mark.parametrize("inputs",
                         ['i8', 'cursor(i8,f8)', 'i8;f8', 'i4;cursor(f8,i8)',
                          'cursor(i4,i8);f8', 'cursor(i4,i8);cursor(f8,f4)',
                          'cursor(i4,i8,f8,f4)'])
def test_copy(heavydb, use_default, inputs):
    if use_default:
        heavydb.require_version((5, 7), 'Requires heavydb-internal PR 5403')

    groups = inputs.split(';')
    table_names = [f'{heavydb.table_name}'] * len(groups)

    group_results = {}
    args = []
    cc = []
    expected = [()] * 5
    for colnames, table_name in zip(groups, table_names):
        if colnames.startswith('cursor('):
            colnames = colnames[7:-1]
        result = group_results.get(colnames)
        if result is None:
            _, result = heavydb.sql_execute(f'select {colnames} from {table_name}')
            result = group_results[colnames] = list(result)
        expected = [row1 + row2 for row1, row2 in zip(expected, result)]
        args.append(f'cursor(select {colnames} from {table_name})')
        cc.append((colnames.count(',') + 1) * 'c')
    if not use_default:
        args.append('1')
    args = ', '.join(args)
    cc = '_'.join(cc)

    query = f'select * from table(text_rbc_copy_{cc}_rowmul({args}))'
    _, result = heavydb.sql_execute(query)
    result = list(result)

    assert result == expected


@pytest.mark.parametrize('kind', ['1', '11', '111', '211', '221', '212', '13', '32', '34', '242'])
def test_ct_binding_constant_sizer(heavydb, kind):
    heavydb.require_version((5, 7), 'Requires heavydb-internal PR 5274')
    colnames = ', '.join({'1': 'i4', '2': 'i8', '3': 'i4, i4, i4',
                          '4': 'i8, i8, i8'}[n] for n in kind)

    query = (f'select * from table(ct_binding_udtf_constant(cursor('
             f'select {colnames} from {heavydb.table_name})))')
    _, result = heavydb.sql_execute(query)
    result = list(result)

    assert result == [(int(kind),)]


@pytest.mark.parametrize('use_default', [True, False])
@pytest.mark.parametrize('kind', ['19', '119', '1119', '2119', '2219',
                                  '2129', '139', '329', '349', '2429',
                                  '91', '196', '396', '369', '169'])
def test_ct_binding_row_multiplier(heavydb, use_default, kind):
    if use_default:
        heavydb.require_version((5, 7), 'Requires heavydb-internal PR 5403')

    if heavydb.version < (5, 7):
        suffix = {'91': '2', '369': '2', '169': '3'}.get(kind, '')
        codes = {'1': 'i4', '2': 'i8', '3': 'i4, i4, i4', '4': 'i8, i8, i8',
                 '9': '1', '6': 'cast(123 as int)'}
        first = []
        last = []
        cursor = []
        for n in kind:
            if n in '69':
                if cursor:
                    last.append(codes[n])
                else:
                    first.append(codes[n])
            else:
                assert n in '1234'
                cursor.append(codes[n])
        first = ', '.join(first + [''])
        last = ', '.join([''] + last)
        cursor = ', '.join(cursor)
        query = (f'select * from table(ct_binding_udtf{suffix}({first}cursor('
                 f'select {cursor} from {heavydb.table_name}){last}))')
        _, result = heavydb.sql_execute(query)
        result = list(result)

        assert result == [(1000 + int(kind),)], (result, query)
    else:
        suffix = {'91': '2', '369': '5', '169': '3', '196': '6', '396': '4'}.get(kind, '')
        codes = {'1': 'i4', '2': 'i8', '3': 'i4, i4, i4', '4': 'i8, i8, i8',
                 '9': '1', '6': 'cast(123 as int)'}
        input_val = 123 if suffix and suffix in '6543' else 0
        first = []
        last = []
        cursor = []
        for n in kind:
            if n == '6':
                if cursor:
                    last.append(codes[n])
                else:
                    first.append(codes[n])
            elif n == '9':
                if use_default:
                    continue
                if cursor:
                    last.append(codes[n])
                else:
                    first.append(codes[n])
            else:
                assert n in '1234'
                cursor.append(codes[n])
        first = ', '.join(first + [''])
        last = ', '.join([''] + last)
        cursor = ', '.join(cursor)
        query = (f'select * from table(ct_binding_udtf{suffix}({first}cursor('
                 f'select {cursor} from {heavydb.table_name}){last}))')
        _, result = heavydb.sql_execute(query)
        result = list(result)

        assert result == [(1000 + int(kind) + 1 + 10*input_val,)], (result, query)
