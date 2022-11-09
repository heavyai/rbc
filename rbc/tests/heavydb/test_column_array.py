import pytest
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 3), suffices=['array']):
        define(o)
        yield o


def define(heavydb):

    @heavydb("int32(TableFunctionManager, Column<Array<T>> inp, OutputColumn<Array<T>> out)", T=['int64'])
    def test_column_array(mgr, inp, out):
        sz = len(inp)
        output_values_size = 0
        # for i in range(sz):
        #     output_values_size += len(inp[i])
        output_values_size += len(inp[0])

        # # initialize array buffers
        # mgr.set_output_array_values_total_number(0, output_values_size)

        out.is_null(0)
        # mgr.set_output_row_size(sz)
        # for i in range(sz):
        #     if inp.is_null(i):
        #         out.set_null(i)
        #     else:
        #         out.set_item(i, inp[i])
        return sz

    heavydb.register()


@pytest.mark.parametrize("col", ['i8'])
def test_copy(heavydb, col):
    query = (f'select * from table(test_column_array(cursor(select {col} '
             f'from {heavydb.table_name}array)));')
    # _, result = heavydb.sql_execute(query)
    # print(list(result))