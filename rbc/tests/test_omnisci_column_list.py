import pytest
from rbc.tests import omnisci_fixture

scalar_types = ['float64', 'int64', 'float32', 'int32', 'int16', 'int8']
table_columns_map = dict(int64='i8', int32='i4', int16='i2', int8='i1',
                         float64='f8', float32='f4')
sql_type_map = dict(int64='BIGINT', int32='INT', int16='SMALLINT', int8='TINYINT',
                    float64='DOUBLE', float32='FLOAT')


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 6)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('int32 columns_sum1(Cursor<Column<int64>, ColumnList<T>>,'
             ' RowMultiplier, OutputColumn<T>)',
             T=scalar_types, devices=['cpu'])
    def columns_sum1(rowid, lst, m, out):
        for i in range(lst.size):
            out[i] *= 0
            for j in range(lst.length):
                out[i] += lst.ptrs[j][i]
        return lst.size

    @omnisci('int32 columns_sum2(Cursor<Column<int64>, ColumnList<T>>,'
             ' RowMultiplier, OutputColumn<T>)',
             T=scalar_types, devices=['cpu'])
    def columns_sum2(rowid, lst, m, out):
        for j in range(lst.length):
            col = lst[j]  # equivalent to lst.ptrs[j]
            if j == 0:
                for i in range(lst.size):
                    out[i] = col[i]
            else:
                for i in range(lst.size):
                    out[i] += col[i]
        return lst.size

    @omnisci('int32 columns_sum3(Cursor<Column<int64>, ColumnList<T>>,'
             ' RowMultiplier, OutputColumn<T>)',
             T=scalar_types, devices=['cpu'])
    def columns_sum3(rowid, lst, m, out):
        for j in range(lst.ncols):
            col = lst[j]
            for i in range(lst.nrows):
                out[i] = col[i] if j == 0 else out[i] + col[i]
        return lst.nrows


@pytest.mark.parametrize("variant", ['1', "2", "3"])
@pytest.mark.parametrize("T", scalar_types)
def test_columns_sum(omnisci, T, variant):
    v = table_columns_map[T]
    c = sql_type_map[T]

    query = f'select rowid, ({v} + {v} + 1) from {omnisci.table_name} ORDER BY rowid'
    _, result = omnisci.sql_execute(query)
    expected = list(zip(*result))[1]

    # using `.. ORDER BY rowid LIMIT ALL` will crash omniscidb server
    query = (f'select * from table(columns_sum{variant}(cursor('
             f'select rowid, {v}, CAST({v}+1 as {c}) '
             f'from {omnisci.table_name} ORDER BY rowid LIMIT 1000), 1))')
    _, result = omnisci.sql_execute(query)
    actual = list(zip(*result))[0]

    assert expected == actual
