# tests for how-tos page
# ensure that examples in the docs page are correct
import pytest
from rbc.errors import HeavyDBServerError
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals()):
        yield o


def test_connect(heavydb):
    # magictoken.connect.begin
    from rbc.heavydb import RemoteHeavyDB
    heavy = RemoteHeavyDB(user='admin', password='HyperInteractive',
                          host='127.0.0.1', port=6274)
    # magictoken.connect.end
    assert heavy.version is not None


def test_external_functions(heavydb):
    # magictoken.external_functions.abs.begin
    from rbc.external import external
    cmath_abs = external('int64 abs(int64)')

    @heavydb('int64(int64)')
    def apply_abs(x):
        return cmath_abs(x)

    _, result = heavydb.sql_execute('SELECT apply_abs(-3);')
    # magictoken.external_functions.abs.end
    assert list(result) == [(3,)]

    # magictoken.external_functions.printf.begin
    from rbc.externals.stdio import printf

    @heavydb('int64(int64)')
    def power_2(x):
        # This message will show in the heavydb logs
        printf("input number: %d\n", x)
        return x * x

    _, result = heavydb.sql_execute('SELECT power_2(3);')
    # magictoken.external_functions.printf.end
    assert list(result) == [(9,)]


def test_raise_exception(heavydb):
    # magictoken.raise_exception.begin
    @heavydb('int32(TableFunctionManager, Column<int>, OutputColumn<int>)')
    def udtf_copy(mgr, inp, out):
        size = len(inp)
        if size > 4:
            # error message must be known at compile-time
            return mgr.error_message('TableFunctionManager error_message!')

        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = inp[i]
        return size
    # magictoken.raise_exception.end

    col = 'i4'
    table_name = heavydb.table_name

    query = f'''
        SELECT
          *
        FROM
            TABLE(udtf_copy(
                cursor(SELECT {col} FROM {table_name})
            ))
    '''

    with pytest.raises(HeavyDBServerError) as exc:
        heavydb.sql_execute(query)
    exc_msg = ('Error executing table function: TableFunctionManager '
               'error_message!')
    assert exc.match(exc_msg)


def test_devices(heavydb):
    # magictoken.devices.begin
    @heavydb('int32(int32, int32)', devices=['CPU', 'GPU'])
    def add(a, b):
        return a + b
    # magictoken.devices.end
    _, result = heavydb.sql_execute('SELECT add(-3, 3);')
    assert list(result) == [(0,)]


def test_templates(heavydb):
    heavydb.unregister()

    # magictoken.templates.begin
    @heavydb('Z(T, Z)', T=['int32', 'float'], Z=['int64', 'double'])
    def add(a, b):
        return a + b

    heavydb.register()
    assert heavydb.function_names(runtime_only=True) == ['add']
    assert len(heavydb.function_details('add')) == 4
    # magictoken.templates.end


def test_udf(heavydb):
    # magictoken.udf.begin
    @heavydb('int64(int64)')
    def incr(a):
        return a + 1
    # magictoken.udf.end

    _, result = heavydb.sql_execute('SELECT incr(3)')
    assert list(result) == [(4,)]

    heavydb.unregister()

    # magictoken.udf.multiple_signatures.begin
    @heavydb('int64(int64, int64)', 'float64(float64, float64)')
    def multiply(a, b):
        return a * b
    # magictoken.udf.multiple_signatures.end
    _, result = heavydb.sql_execute('SELECT multiply(3.0, 2.0)')
    assert list(result) == [(6.0,)]


def test_udtf(heavydb):
    col = 'f4'
    table_name = heavydb.table_name

    # magictoken.udtf.begin
    @heavydb('int32(TableFunctionManager, Column<float>, OutputColumn<float>)')
    def my_copy(mgr, inp, out):
        size = len(inp)
        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = inp[i]
        return size
    # magictoken.udtf.end

    query = f'''
        SELECT
            *
        FROM
            TABLE(my_copy(
                cursor(SELECT {col} FROM {table_name})
            ))
    '''
    _, result = heavydb.sql_execute(query)
    assert list(result) == [(0.0,), (1.0,), (2.0,), (3.0,), (4.0,)]


def test_udf_text(heavydb):
    table_name = heavydb.table_name + 'text'

    if heavydb.version[:2] >= (6, 3):
        # magictoken.udf.text.dict.begin
        # Requires HeavyDB server v6.3 or newer
        @heavydb('TextEncodingDict(RowFunctionManager, TextEncodingDict)')
        def text_copy(mgr, t):
            db_id: int = mgr.getDictDbId('text_copy', 0)
            dict_id: int = mgr.getDictId('text_copy', 0)
            s: str = mgr.getString(db_id, dict_id, t)
            return mgr.getOrAddTransient(
                mgr.TRANSIENT_DICT_DB_ID,
                mgr.TRANSIENT_DICT_ID,
                s)
        # magictoken.udf.text.dict.end
        query = f"select text_copy(t1) from {table_name}"
        _, r = heavydb.sql_execute(query)
        assert list(r) == [('fun',), ('bar',), ('foo',), ('barr',), ('foooo',)]

        # magictoken.udf.text.none.begin
        from rbc.heavydb import TextEncodingNone

        @heavydb('TextEncodingNone(TextEncodingNone)')
        def text_duplicate(t):
            s: str = t.to_string()
            return TextEncodingNone(s + s)
        # magictoken.udf.text.none.end

        query = f"select text_duplicate(n) from {table_name}"
        _, r = heavydb.sql_execute(query)
        assert list(r) == [('funfun',), ('barbar',), ('foofoo',), ('barrbarr',),
                           ('foooofoooo',)]


def test_array(heavydb):
    table_name = heavydb.table_name + 'array'

    # magictoken.udf.array.new.begin
    from rbc.heavydb import Array

    @heavydb('Array<int64>(int32)')
    def arr_new(size):
        arr = Array(size, dtype='int64')
        for i in range(size):
            arr[i] = 1
        return arr
    # magictoken.udf.array.new.end

    _, r = heavydb.sql_execute('SELECT arr_new(5);')
    assert list(r) == [([1, 1, 1, 1, 1],)]

    # magictoken.udf.array.length.begin
    @heavydb('int64(Array<int32>)')
    def my_length(arr):
        return len(arr)
    # magictoken.udf.array.length.end

    _, r = heavydb.sql_execute(f'SELECT my_length(i4) from {table_name}')
    assert list(r) == [(0,), (1,), (2,), (3,), (4,)]

    # magictoken.udf.array.array_api.begin
    from rbc.stdlib import array_api

    @heavydb('Array<int64>(int32)')
    def arr_new_ones(sz):
        return array_api.ones(sz, dtype='int64')
    # magictoken.udf.array.array_api.end

    _, r = heavydb.sql_execute('SELECT arr_new_ones(5);')
    assert list(r) == [([1, 1, 1, 1, 1],)]