# tests for how-tos page
# ensure that examples in the docs page are correct
import pytest
from rbc.errors import HeavyDBServerError
from rbc.tests import heavydb_fixture
from rbc.heavydb import RemoteHeavyDB


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
    heavydb.unregister()
    # magictoken.external_functions.abs.begin
    from rbc.external import external
    cmath_abs = external('int64 abs(int64)')

    @heavydb('int64(int64)')
    def apply_abs(x):
        return cmath_abs(x)
    # magictoken.external_functions.abs.end

    # magictoken.external_functions.abs.sql.begin
    assert apply_abs(-3).execute() == 3
    # magictoken.external_functions.abs.sql.end

    # magictoken.external_functions.printf.begin
    from rbc.externals.stdio import printf

    @heavydb('int64(int64)')
    def power_2(x):
        # This message will show in the heavydb logs
        printf("input number: %d\n", x)
        return x * x
    # magictoken.external_functions.printf.end

    # magictoken.external_functions.printf.sql.begin
    assert power_2(3).execute() == 9
    # magictoken.external_functions.printf.sql.end


def test_raise_exception(heavydb):
    heavydb.unregister()

    # magictoken.raise_exception.begin
    @heavydb('int32(TableFunctionManager, Column<int64>, OutputColumn<int64>)')
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

    # magictoken.raise_exception.sql.begin
    query = '''
        SELECT * FROM TABLE(udtf_copy(
            cursor(SELECT * FROM TABLE(generate_series(1, 5, 1)))
        ));
    '''

    with pytest.raises(HeavyDBServerError) as exc:
        heavydb.sql_execute(query)
    exc_msg = ('Error executing table function: TableFunctionManager '
               'error_message!')
    assert exc.match(exc_msg)
    # magictoken.raise_exception.sql.end


def test_devices(heavydb):
    heavydb.unregister()

    # magictoken.devices.begin
    @heavydb('int32(int32, int32)', devices=['CPU', 'GPU'])
    def add(a, b):
        return a + b
    # magictoken.devices.end

    # magictoken.devices.sql.begin
    _, result = heavydb.sql_execute('SELECT add(-3, 3);')
    assert list(result) == [(0,)]
    # magictoken.devices.sql.end


def test_templates(heavydb):
    heavydb.unregister()

    # magictoken.templates.begin
    @heavydb('Z(T, Z)', T=['int32', 'float'], Z=['int64', 'double'])
    def add(a, b):
        return a + b
    # magictoken.templates.end
    heavydb.register()

    # magictoken.templates.sql.begin
    if heavydb.version[:2] >= (6, 2):
        assert heavydb.function_names(runtime_only=True) == ['add']
        assert len(heavydb.function_details('add')) == 4
    assert add(2, 3).execute() == 5
    # magictoken.templates.sql.end


def test_udf(heavydb):
    heavydb.unregister()

    # magictoken.udf.begin
    @heavydb('int64(int64)')
    def incr(a):
        return a + 1
    # magictoken.udf.end

    # magictoken.udf.sql.begin
    _, result = heavydb.sql_execute('SELECT incr(3)')
    assert list(result) == [(4,)]
    # magictoken.udf.sql.end

    heavydb.unregister()

    # magictoken.udf.multiple_signatures.begin
    @heavydb('int64(int64, int64)', 'float64(float64, float64)')
    def multiply(a, b):
        return a * b
    # magictoken.udf.multiple_signatures.end

    # magictoken.udf.multiple_signatures.sql.begin
    _, result = heavydb.sql_execute('SELECT multiply(3.0, 2.0)')
    assert list(result) == [(6.0,)]
    # magictoken.udf.multiple_signatures.sql.end


def test_udtf(heavydb):
    heavydb.unregister()

    # magictoken.udtf.begin
    @heavydb('int32(TableFunctionManager, Column<int64>, OutputColumn<int64>)')
    def my_copy(mgr, inp, out):
        size = len(inp)
        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = inp[i]
        return size
    # magictoken.udtf.end

    # magictoken.udtf.sql.begin
    query = '''
        SELECT * FROM TABLE(my_copy(
            cursor(SELECT * FROM TABLE(generate_series(1, 5, 1)))
        ));
    '''
    _, result = heavydb.sql_execute(query)
    assert list(result) == [(1,), (2,), (3,), (4,), (5,)]
    # magictoken.udtf.sql.end


def test_udf_text_copy(heavydb):
    heavydb.unregister()
    if heavydb.version[:2] >= (6, 3):
        # magictoken.udf.text.dict.begin
        # Requires HeavyDB server v6.3 or newer
        @heavydb('TextEncodingDict(RowFunctionManager, TextEncodingDict)')
        def text_copy(mgr, t):
            db_id: int = mgr.get_dict_db_id('text_copy', 0)
            dict_id: int = mgr.get_dict_id('text_copy', 0)
            s: str = mgr.get_string(db_id, dict_id, t)
            return mgr.get_or_add_transient(
                mgr.TRANSIENT_DICT_DB_ID,
                mgr.TRANSIENT_DICT_ID,
                s)
        # magictoken.udf.text.dict.end

        table_name = heavydb.table_name + 'text'
        query = f"select text_copy(t1) from {table_name}"
        _, r = heavydb.sql_execute(query)
        assert list(r) == [('fun',), ('bar',), ('foo',), ('barr',), ('foooo',)]


def test_udf_dict_proxy(heavydb):
    heavydb.unregister()
    if heavydb.version[:2] >= (6, 3):
        # magictoken.udf.proxy.begin
        # Requires HeavyDB server v6.3 or newer
        from rbc.heavydb import StringDictionaryProxy

        @heavydb('TextEncodingDict(RowFunctionManager, TextEncodingDict)')
        def test_string_proxy(mgr, t):
            db_id: int = mgr.get_dict_db_id('test_string_proxy', 0)
            dict_id: int = mgr.get_dict_id('test_string_proxy', 0)
            proxy: StringDictionaryProxy = mgr.get_string_dictionary_proxy(db_id, dict_id)
            s: str = proxy.get_string(t)
            return mgr.get_or_add_transient(
                mgr.TRANSIENT_DICT_DB_ID,
                mgr.TRANSIENT_DICT_ID,
                s)
        # magictoken.udf.proxy.end

        table_name = heavydb.table_name + 'text'
        query = f"select test_string_proxy(t1) from {table_name}"
        _, r = heavydb.sql_execute(query)
        assert list(r) == [('fun',), ('bar',), ('foo',), ('barr',), ('foooo',)]


def test_udtf_string_proxy(heavydb):
    heavydb.unregister()

    if heavydb.version[:2] < (6, 2):
        pytest.skip('Test requires HeavyDB 6.1 or newer')

    # magictoken.udtf.proxy.begin
    @heavydb('int32(TableFunctionManager, Column<T>, OutputColumn<T> | input_id=args<0>)',
             T=['TextEncodingDict'])
    def test_string_proxy(mgr, inp, out):
        size = len(inp)
        mgr.set_output_row_size(size)
        for i in range(size):
            s = inp.string_dict_proxy.get_string(inp[i])
            id = out.string_dict_proxy.get_or_add_transient(s.title())
            out[i] = id
        return size
    # magictoken.udtf.proxy.end

    table_name = heavydb.table_name + 'text'
    query = f'''
        SELECT * FROM TABLE(test_string_proxy(
            cursor(SELECT t1 from {table_name})
        ))
    '''
    _, r = heavydb.sql_execute(query)
    assert list(r) == [('Fun',), ('Bar',), ('Foo',), ('Barr',), ('Foooo',)]


def test_udf_text_duplicate(heavydb):
    heavydb.unregister()
    # magictoken.udf.text.none.begin
    from rbc.heavydb import TextEncodingNone

    @heavydb('TextEncodingNone(TextEncodingNone)')
    def text_duplicate(t):
        s: str = t.to_string()
        return TextEncodingNone(s + s)
    # magictoken.udf.text.none.end

    table_name = heavydb.table_name + 'text'
    query = f"select text_duplicate(n) from {table_name}"
    _, r = heavydb.sql_execute(query)
    assert list(r) == [('funfun',), ('barbar',), ('foofoo',), ('barrbarr',),
                       ('foooofoooo',)]


def test_udf_text_capitalize(heavydb):
    heavydb.unregister()
    # magictoken.udf.text.capitalize.begin
    from rbc.heavydb import TextEncodingNone

    @heavydb('TextEncodingNone(TextEncodingNone)')
    def text_capitalize(t):
        s: str = t.to_string()
        return TextEncodingNone(s.capitalize())
    # magictoken.udf.text.capitalize.end

    table_name = heavydb.table_name + 'text'
    query = f"select text_capitalize(n) from {table_name}"
    _, r = heavydb.sql_execute(query)
    assert list(r) == [('Fun',), ('Bar',), ('Foo',), ('Barr',),
                       ('Foooo',)]


def test_array(heavydb):
    heavydb.unregister()
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

    # magictoken.udf.array.new.sql.begin
    _, r = heavydb.sql_execute('SELECT arr_new(5);')
    assert list(r) == [([1, 1, 1, 1, 1],)]
    # magictoken.udf.array.new.sql.end

    # magictoken.udf.array.length.begin
    @heavydb('int64(Array<int32>)')
    def my_length(arr):
        return len(arr)
    # magictoken.udf.array.length.end

    # magictoken.udf.array.length.sql.begin
    _, r = heavydb.sql_execute(f'SELECT my_length(i4) from {table_name}')
    assert list(r) == [(0,), (1,), (2,), (3,), (4,)]
    # magictoken.udf.array.length.sql.end

    # magictoken.udf.array.array_api.begin
    from rbc.stdlib import array_api

    @heavydb('Array<int64>(int32)')
    def arr_new_ones(sz):
        return array_api.ones(sz, dtype='int64')
    # magictoken.udf.array.array_api.end

    # magictoken.udf.array.array_api.sql.begin
    _, r = heavydb.sql_execute('SELECT arr_new_ones(5);')
    assert list(r) == [([1, 1, 1, 1, 1],)]
    # magictoken.udf.array.array_api.sql.end


def test_tablefunctionmanager(heavydb):
    heavydb.unregister()

    # magictoken.udtf.mgr.basic.begin
    @heavydb('int32(TableFunctionManager, Column<int64>, OutputColumn<int64>)')
    def table_copy(mgr, inp, out):
        size = len(inp)
        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = inp[i]
        return size
    # magictoken.udtf.mgr.basic.end

    # magictoken.udtf.mgr.basic.sql.begin
    query = '''
        SELECT * FROM TABLE(table_copy(
            cursor(SELECT * FROM TABLE(generate_series(0, 4, 1)))
        ))
    '''
    _, r = heavydb.sql_execute(query)
    assert list(r) == [(0,), (1,), (2,), (3,), (4,)]
    # magictoken.udtf.mgr.basic.sql.end


def test_rowfunctionmanager(heavydb):
    heavydb.unregister()
    table = heavydb.table_name + 'text'

    if heavydb.version[:2] >= (6, 4):
        # magictoken.udf.mgr.basic.begin
        @heavydb('TextEncodingDict(RowFunctionManager, TextEncodingDict)')
        def concat(mgr, text):
            db_id: int = mgr.get_dict_db_id('concat', 0)
            dict_id: int = mgr.get_dict_id('concat', 0)
            s: str = mgr.get_string(db_id, dict_id, text)
            s_concat = 'test: ' + s
            return mgr.get_or_add_transient(mgr.TRANSIENT_DICT_DB_ID,
                                            mgr.TRANSIENT_DICT_ID,
                                            s_concat)
        # magictoken.udf.mgr.basic.end

        # magictoken.udf.mgr.basic.sql.begin
        query = f'''
            SELECT concat(t1) FROM {table}
        '''
        _, r = heavydb.sql_execute(query)
        assert list(r) == [('test: fun',), ('test: bar',), ('test: foo',),
                           ('test: barr',), ('test: foooo',)]
        # magictoken.udf.mgr.basic.sql.end


def test_column_power(heavydb):
    heavydb.unregister()

    # magictoken.udtf.column.basic.begin
    import numpy as np

    @heavydb('int32(TableFunctionManager, Column<T>, T, OutputColumn<T>)',
             T=['int64'])
    def udtf_power(mgr, inp, exp, out):
        size = len(inp)
        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = np.power(inp[i], exp)
        return size
    # magictoken.udtf.column.basic.end

    # magictoken.udtf.column.basic.sql.begin
    query = '''
        SELECT * FROM TABLE(udtf_power(
            cursor(SELECT * FROM TABLE(generate_series(1, 5, 1))),
            3
        ))
    '''
    _, r = heavydb.sql_execute(query)
    assert list(r) == [(1,), (8,), (27,), (64,), (125,)]
    # magictoken.udtf.column.basic.sql.end


def test_geopoint(heavydb: RemoteHeavyDB):
    if heavydb.version[:2] < (7, 0):
        pytest.skip('Requires HeavyDB 7.0 or newer')

    heavydb.unregister()

    # magictoken.udtf.geopoint.basic.begin
    from rbc.heavydb import Point2D

    @heavydb('int32(TableFunctionManager, int64 size, OutputColumn<Z>)',
             Z=['GeoPoint'])
    def generate_geo(mgr, size, out):
        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = Point2D(i, i)
        return size
    # magictoken.udtf.geopoint.basic.end

    # magictoken.udtf.geopoint.basic.sql.begin
    query = '''
        SELECT * FROM TABLE(
            generate_geo(5)
        );
    '''
    _, r = heavydb.sql_execute(query)
    r = list(r)
    assert r == [('POINT (0 0)',), ('POINT (1 1)',), ('POINT (2 2)',),
                 ('POINT (3 3)',), ('POINT (4 4)',)]
    # magictoken.udtf.geopoint.basic.sql.end


def test_geomultipoint(heavydb: RemoteHeavyDB):
    if heavydb.version[:2] < (7, 0):
        pytest.skip('Requires HeavyDB 7.0 or newer')

    heavydb.unregister()

    # magictoken.udtf.mp.basic.begin
    @heavydb('int32(TableFunctionManager, int64 size, OutputColumn<Z>)',
             Z=['GeoMultiPoint'])
    def generate_geo(mgr, size, out):
        mgr.set_output_item_values_total_number(0, size * 4)
        mgr.set_output_row_size(size)
        for i in range(size):
            coords = [i + 1.0, i + 2.0, i + 3.0, i + 4.0]
            out[i].from_coords(coords)
        return size
    # magictoken.udtf.mp.basic.end

    # magictoken.udtf.mp.basic.sql.begin
    query = '''
        SELECT * FROM TABLE(
            generate_geo(3)
        );
    '''
    _, r = heavydb.sql_execute(query)
    r = list(r)
    assert r == [('MULTIPOINT (1 2,3 4)',), ('MULTIPOINT (2 3,4 5)',),
                 ('MULTIPOINT (3 4,5 6)',)]
    # magictoken.udtf.mp.basic.sql.end
