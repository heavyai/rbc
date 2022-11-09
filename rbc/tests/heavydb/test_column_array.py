import pytest
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 3),
                             suffices=['array', 'arraynull', 'text']):
        define(o)
        yield o


def define(heavydb):
    @heavydb("int32(TableFunctionManager, Column<Array<T>> inp, OutputColumn<Array<T>> out | input_id=args<0>)",  # noqa: E501
             T=['bool', 'int8', 'int16', 'int32', 'int64', 'float', 'double', 'TextEncodingDict'],
             devices=['cpu'])
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


@pytest.mark.parametrize("suffix", ['array', 'arraynull'])
@pytest.mark.parametrize("col", ['b', 'i1', 'i2', 'i4', 'i8', 'f4', 'f8'])
def test_copy(heavydb, suffix, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB 6.3 or newer')

    query = (f'select * from table(test_column_array(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select {col} from {heavydb.table_name}{suffix}')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    assert len(result) == len(expected) == 5
    assert result == expected


@pytest.mark.parametrize("col", ['s'])
def test_copy_text_encoding_dict(heavydb, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB 6.3 or newer')

    query = (f'select * from table(test_column_array(cursor(select {col} '
             f'from {heavydb.table_name}text)));')
    _, result = heavydb.sql_execute(query)

    query = (f'select {col} from {heavydb.table_name}text')
    _, expected = heavydb.sql_execute(query)
    result = list(result)
    expected = list(expected)
    assert len(result) == len(expected) == 5
    assert result == expected
