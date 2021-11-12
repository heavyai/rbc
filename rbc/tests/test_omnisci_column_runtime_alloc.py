import pytest
from rbc.tests import omnisci_fixture
from rbc.externals.omniscidb import set_output_row_size


scalar_types = ['float64', 'int64', 'float32', 'int32', 'int16', 'int8']
table_columns_map = dict(int64='i8', int32='i4', int16='i2', int8='i1',
                         float64='f8', float32='f4')


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 7, 1)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('int32(Column<T>, OutputColumn<T>)', T=scalar_types, devices=['cpu'])
    def column_set_output_row_size(inp, out):
        sz = len(inp)
        set_output_row_size(sz)
        for i in range(sz):
            out[i] = inp[i]
        return sz

    @omnisci('int32(ColumnList<T>, OutputColumn<T>)', T=['int32'], devices=['cpu'])
    def columnlist_set_output_row_size(lst, out):
        set_output_row_size(lst.ncols)
        for j in range(lst.ncols):
            col = lst[j]
            out[j] = 0
            for i in range(lst.nrows):
                out[j] += col[i]
        return len(out)


@pytest.mark.parametrize('T', table_columns_map)
def test_column_alloc(omnisci, T):
    fn = 'column_set_output_row_size'
    col = table_columns_map[T]

    query = (f'select * from table({fn}(cursor('
             f'select {col} from {omnisci.table_name})));')

    _, result = omnisci.sql_execute(query)

    if 'f' in col:
        expected = [(0.0,), (1.0,), (2.0,), (3.0,), (4.0,)]
    else:
        expected = [(0,), (1,), (2,), (3,), (4,)]
    assert list(result) == expected


@pytest.mark.parametrize("fn", ('column_list_row_sum', 'columnlist_set_output_row_size'))
def test_columnlist_alloc(omnisci, fn):

    query = (f'select * from table({fn}(cursor('
             f'select i4, i4+1, i4+2 from {omnisci.table_name})));')

    _, result = omnisci.sql_execute(query)

    assert list(result) == [(10,), (15,), (20,)]
