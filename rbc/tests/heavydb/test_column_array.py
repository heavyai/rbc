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
             T=['float', 'int64', 'TextEncodingDict'], devices=['cpu'])
    def rbc_array_copy(mgr, inp, out):
        sz = len(inp)
        output_values_size = 0
        for i in range(sz):
            output_values_size += len(inp[i])

        # initialize array buffers
        mgr.set_output_array_values_total_number(0, output_values_size)

        mgr.set_output_row_size(sz)
        for i in range(sz):
            if inp.is_null(i):
                out.set_null(i)
            else:
                out.set_item(i, inp[i])
        return sz

    @heavydb('int32(TableFunctionManager, ColumnList<Array<T>> lst, OutputColumn<Array<T>> out | input_id=args<0>)',  # noqa: E501
             T=['int64', 'float', 'TextEncodingDict'], devices=['cpu'])
    def rbc_array_concat(mgr, lst, out):
        output_values_size = 0

        for j in range(lst.ncols):
            for i in range(lst.nrows):
                output_values_size += len(lst[j][i])

        mgr.set_output_array_values_total_number(
            0,  # output column index
            output_values_size,  # upper bound to the number of items
                                 # in all output arrays
        )

        size = lst.nrows
        mgr.set_output_row_size(size)

        for i in range(lst.nrows):
            for j in range(lst.ncols):
                col = lst[j]
                arr = col[i]
                out.concat_item(i, arr)

        return size


@pytest.mark.parametrize("suffix", ['array'])
@pytest.mark.parametrize("col", ['i8', 'f4'])
def test_copy(heavydb, suffix, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB 6.3 or newer')

    query = (f'select * from table(rbc_array_copy(cursor(select {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select {col} from {heavydb.table_name}{suffix}')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    assert len(result) == len(expected) == 5
    assert result == expected


@pytest.mark.parametrize("col", ['s', 'na'])
def test_copy_text(heavydb, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB 6.3 or newer')

    if col == 'na':
        pytest.skip('Column<Array<TextEncodingNone>> is not supported yet.')

    query = (f'select * from table(rbc_array_copy(cursor(select {col} '
             f'from {heavydb.table_name}text)));')
    _, result = heavydb.sql_execute(query)

    query = (f'select {col} from {heavydb.table_name}text')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    assert len(result) == len(expected) == 5
    assert result == expected


@pytest.mark.parametrize("suffix", ['array', 'arraynull'])
@pytest.mark.parametrize("col", ['i8', 'f4'])
def test_concat(heavydb, suffix, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB 6.3 or newer')

    query = (f'select * from table(rbc_array_concat(cursor(select {col}, {col} '
             f'from {heavydb.table_name}{suffix})));')
    _, result = heavydb.sql_execute(query)

    query = (f'select {col}, {col} from {heavydb.table_name}{suffix}')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    assert len(result) == len(expected) == 5
    for a, (b, c) in zip(result, expected):
        if a == (None,):
            assert b is None and c is None
        else:
            assert a == (b + c,)


@pytest.mark.parametrize("col", ['s', 'na'])
def test_concat_text(heavydb, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB 6.3 or newer')

    if col == 'na':
        pytest.skip('Column<Array<TextEncodingNone>> is not supported yet.')

    query = (f'select * from table(rbc_array_concat(cursor(select {col} '
             f'from {heavydb.table_name}text)));')
    _, result = heavydb.sql_execute(query)

    query = (f'select {col} from {heavydb.table_name}text')
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    assert len(result) == len(expected) == 5
    assert result == expected
