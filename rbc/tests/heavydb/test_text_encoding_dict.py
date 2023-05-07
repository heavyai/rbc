import os
import pytest
import itertools
from rbc.tests import heavydb_fixture
from rbc.heavydb import TextEncodingNone


@pytest.fixture(scope="module")
def heavydb():

    for o in heavydb_fixture(globals(), minimal_version=(5, 7), suffices=['', 'text']):
        define(o)
        o.base_name = os.path.splitext(os.path.basename(__file__))[0]
        yield o


def define(heavydb):
    @heavydb("int32(Column<T>, RowMultiplier, OutputColumn<T> | input_id=args<0>)",
             T=["TextEncodingDict"])
    def test_shared_dict_copy(x, m, y):
        sz = len(x)
        for i in range(sz):
            y[i] = x[i]
        return m * sz

    @heavydb("int32(Column<T>, Column<T>, Column<T>, RowMultiplier, "
             "OutputColumn<T> | input_id=args<1>, "
             "OutputColumn<T> | input_id=args<0>, "
             "OutputColumn<T> | input_id=args<2>)",
             T=["TextEncodingDict"])
    def test_shared_dict_copy2(x, y, z, m, a, b, c):
        sz = len(x)
        for i in range(sz):
            a[i] = y[i]
            b[i] = x[i]
            c[i] = z[i]
        return m * sz

    @heavydb("int32(ColumnList<TextEncodingDict>, RowMultiplier, OutputColumn<int32_t>)",
             devices=['CPU'])
    def test_copy_column_list(lst, m, y):
        for j in range(len(y)):
            y[j] = 0

        for i in range(lst.ncols):
            col = lst[i]
            for j in range(lst.nrows):
                y[j] += col[j]
        return lst.nrows

    @heavydb("int32(ColumnList<T>, Column<int32_t>, ColumnList<T>, RowMultiplier, "
             "OutputColumn<T> | input_id=args<0, 2>, "
             "OutputColumn<T> | input_id=args<2, 0>, "
             "OutputColumn<T> | input_id=args<2, 1>)",
             devices=['CPU'], T=["TextEncodingDict"])
    def test_copy_column_list2(lst1, col, lst2, m, a, b, c):
        for j in range(lst1.nrows):
            a[j] = lst1[2][j]
            b[j] = lst2[0][j]
            c[j] = lst2[1][j]
        return lst1.nrows

    @heavydb("int32(ColumnList<T>, Column<U>, RowMultiplier, OutputColumn<U> | input_id=args<1>)",
             devices=['CPU'], T=["int32_t"], U=["TextEncodingDict"])
    def test_copy_column_list3(lst, col, m, y):
        sz = len(col)
        for j in range(sz):
            y[j] = col[j]
        return m * sz

    if heavydb.version[:2] >= (6, 2):
        @heavydb('int32(Column<TextEncodingDict>, RowMultiplier, TextEncodingNone, OutputColumn<int32_t>)', devices=['CPU'])  # noqa: E501
        def test_getstringid_from_arg(x, m, text, y):
            y[0] = x.string_dict_proxy.getStringId(text)
            return 1

        # No need to specify devices=['CPU'] when a UDTF uses
        # TableFunctionManager argument.
        @heavydb('int32(TableFunctionManager, OutputColumn<TextEncodingDict> | input_id=args<>)')  # noqa: E501
        def test_empty_input_id(mgr, out):
            mgr.set_output_row_size(2)
            out[0] = out.string_dict_proxy.getStringId("onedal")
            out[1] = out.string_dict_proxy.getStringId("mlpack")
            return len(out)

        @heavydb('int32(Column<T>, RowMultiplier, OutputColumn<int32_t>)',
                 T=['TextEncodingDict'],
                 devices=['CPU'])
        def test_getstringid_from_unicode(x, m, y):
            text = "foo"  # this creates a unicode string
            y[0] = x.string_dict_proxy.getStringId(text)
            return 1

        @heavydb('int32(Column<TextEncodingDict>, ConstantParameter, OutputColumn<int32_t>)', devices=['CPU'])  # noqa: E501
        def test_getstring(x, unique, y):
            for i in range(unique):
                y[i] = len(x.string_dict_proxy.getString(i))
            return unique

        @heavydb('int32(ColumnList<TextEncodingDict>, int32_t, RowMultiplier, OutputColumn<int32_t>)', devices=['CPU'])  # noqa: E501
        def test_getstring_lst(lst, unique, m, y):
            for i in range(lst.nrows):
                y[i] = 0

            for i in range(lst.ncols):
                col = lst[i]
                for j in range(unique):
                    y[j] += len(col.string_dict_proxy.getString(j))
            return unique

    if heavydb.version[:2] >= (6, 3):
        # No need to specify devices=['CPU'] when a UDF uses
        # RowFunctionManager argument.
        @heavydb('TextEncodingDict(RowFunctionManager, TextEncodingDict)')
        def fn_copy(mgr, t):
            db_id = mgr.getDictDbId('fn_copy', 0)
            dict_id = mgr.getDictId('fn_copy', 0)
            str = mgr.getString(db_id, dict_id, t)
            return mgr.getOrAddTransient(mgr.TRANSIENT_DICT_DB_ID, mgr.TRANSIENT_DICT_ID, str)

        @heavydb('TextEncodingNone(RowFunctionManager, TextEncodingDict)')
        def to_text_encoding_none_1(mgr, t):
            db_id = mgr.getDictDbId('to_text_encoding_none_1', 0)
            dict_id = mgr.getDictId('to_text_encoding_none_1', 0)
            str = mgr.getString(db_id, dict_id, t)
            return str

        @heavydb('TextEncodingNone(RowFunctionManager, TextEncodingDict)')
        def to_text_encoding_none_2(mgr, t):
            db_id = mgr.getDictDbId('to_text_encoding_none_2', 0)
            dict_id = mgr.getDictId('to_text_encoding_none_2', 0)
            str = mgr.getString(db_id, dict_id, t)
            n = len(str)
            r = TextEncodingNone(n)
            for i in range(n):
                r[i] = str[i]
            return r


@pytest.fixture(scope="module")
def create_columns(heavydb):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    for size in (8, 16, 32):
        table_name = f"{heavydb.base_name}_{size}"
        base = f"base_{size}"
        other = f"other_{size}"
        derived = f"derived_{size}"
        another = f"another_{size}"

        heavydb.sql_execute(f"DROP TABLE IF EXISTS {table_name};")

        heavydb.sql_execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {base} TEXT ENCODING DICT({size}),
                {other} TEXT ENCODING DICT({size}),
                {another} TEXT ENCODING DICT({size}),
                {derived} TEXT,
                SHARED DICTIONARY ({derived}) REFERENCES {table_name}({base})
            );"""
        )

        data = {
            base: ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"],
            other: ["a1", "b2", "c3", "d4", "e5", "f6", "g7"],
            another: ["a", "b", "c", "d", "e", "f", "g"],
            derived: ["world", "bar", "hello", "foo", "baz", "hello", "foo"],
        }

        heavydb.load_table_columnar(table_name, **data)

    yield heavydb

    for size in (8, 16, 32):
        table_name = f"{heavydb.base_name}_{size}"
        heavydb.sql_execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32))
def test_text_encoding_shared_dict(heavydb, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1))"
    _, result = heavydb.sql_execute(query)

    assert list(result) == [
        ("hello",),
        ("foo",),
        ("foofoo",),
        ("world",),
        ("bar",),
        ("foo",),
        ("foofoo",),
    ]


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_text_encoding_shared_dict2(heavydb, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5719")

    fn = "test_shared_dict_copy2"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"
    another = f"another_{size}"

    ans_query = (f"SELECT {other}, {base}, {another} FROM {table};")
    _, ans = heavydb.sql_execute(ans_query)

    query = (f"SELECT * FROM table({fn}("
             f"cursor(SELECT {base} FROM {table}), "
             f"cursor(SELECT {other} FROM {table}), "
             f"cursor(SELECT {another} FROM {table}), "
             "1))")
    _, result = heavydb.sql_execute(query)

    assert list(ans) == list(result)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("col_name", ("base", "other", "another"))
@pytest.mark.parametrize("size", (8, 16, 32))
def test_text_encoding_shared_dict3(heavydb, col_name, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{heavydb.base_name}_{size}"
    col = f"{col_name}_{size}"

    ans_query = (f"SELECT {col} FROM {table};")
    _, ans = heavydb.sql_execute(ans_query)

    query = f"SELECT * FROM table({fn}(cursor(SELECT {col} FROM {table}), 1))"
    _, result = heavydb.sql_execute(query)

    assert list(result) == list(ans)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
@pytest.mark.parametrize("num_column_list", (1, 2, 3))
def test_text_encoding_column_list(heavydb, size, num_column_list):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    fn = "test_copy_column_list"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"
    cols = ', '.join([base] * num_column_list)

    _, expected = heavydb.sql_execute(
        f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1));")

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {cols} FROM {table}), "
        "1));")
    _, result = heavydb.sql_execute(query)

    assert list(map(lambda x: x[0] * num_column_list, expected)) == \
        list(itertools.chain.from_iterable(result))


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (32,))
def test_text_encoding_column_list2(heavydb, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5719")

    fn = "test_copy_column_list2"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"
    another = f"another_{size}"
    cols = f"{base}, {other}, {another}"

    ans_query = (f"SELECT {another}, {base}, {other} FROM {table};")
    _, ans = heavydb.sql_execute(ans_query)

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {cols} FROM {table}), "
        f"cursor(SELECT i4 FROM {heavydb.table_name}), "
        f"cursor(SELECT {cols} FROM {table}), "
        "1));")
    _, result = heavydb.sql_execute(query)

    assert list(ans) == list(result)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (32,))
@pytest.mark.parametrize("num_cols", (1, 2, 3, 4))
def test_text_encoding_column_list3(heavydb, size, num_cols):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5719")
    if heavydb.version >= (6, 0):
        pytest.skip("test_text_encoding_column_list3 crashes heavydb server")

    fn = "test_copy_column_list3"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"
    cols = ", ".join(["i4"] * num_cols)

    _, ans = heavydb.sql_execute(f"SELECT {base} FROM {table} LIMIT 5;")

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {cols} FROM {heavydb.table_name}), "
        f"cursor(SELECT {base} FROM {table}), "
        "1));")
    _, result = heavydb.sql_execute(query)

    assert list(ans) == list(result)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_text_encoding_count(heavydb, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT COUNT(out0) FROM table({fn}(cursor(SELECT {base} FROM {table}), 1))"
    _, result = heavydb.sql_execute(query)

    assert list(result) == [(7,)]


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_text_encoding_order_by(heavydb, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"

    query = (
        f"SELECT out0 FROM table({fn}(cursor(SELECT {base} FROM {table}), 1))"
        " ORDER BY out0;")
    _, result = heavydb.sql_execute(query)

    assert list(result) == [
        ('bar',),
        ('foo',),
        ('foo',),
        ('foofoo',),
        ('foofoo',),
        ('hello',),
        ('world',)
    ]


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_ct_binding_dict_encoded1(heavydb, size):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5492")

    fn = 'ct_binding_dict_encoded1'
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table})));"
    _, result = heavydb.sql_execute(query)
    assert list(result) == [
        ("hello",),
        ("foo",),
        ("foofoo",),
        ("world",),
        ("bar",),
        ("foo",),
        ("foofoo",),
    ]


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
@pytest.mark.parametrize("fn_suffix", ("2", "3",))
def test_ct_binding_dict_encoded23(heavydb, size, fn_suffix):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5719")

    fn = f"ct_binding_dict_encoded{fn_suffix}"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base}, {other} FROM {table})));"
    _, result = heavydb.sql_execute(query)

    out0 = ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"]
    out1 = ["a1", "b2", "c3", "d4", "e5", "f6", "g7"]

    if fn_suffix == "2":
        assert list(result) == list(zip(out0, out1))
    else:
        assert list(result) == list(zip(out1, out0))


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
@pytest.mark.parametrize("fn_suffix", ("4", "5", "6"))
def test_ct_binding_dict_encoded45(heavydb, size, fn_suffix):
    heavydb.require_version((5, 7), "Requires heavydb-internal PR 5719")

    fn = f"ct_binding_dict_encoded{fn_suffix}"
    table = f"{heavydb.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {other}, {base}, {other} FROM {table})));"
    _, result = heavydb.sql_execute(query)

    out4 = ["a1", "b2", "c3", "d4", "e5", "f6", "g7"]
    out5 = ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"]

    if fn_suffix == "4":
        assert list(sum(result, ())) == out4
    elif fn_suffix == "5":
        assert list(sum(result, ())) == out5
    else:
        assert list(result) == list(zip(out4, out5))


@pytest.mark.parametrize('text', ['foo', 'bar', 'invalid1234'])
@pytest.mark.parametrize('col', ['t1', 't2', 't4'])
def test_getStringId(heavydb, col, text):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    fn = 'test_getstringid_from_arg'
    table = f"{heavydb.base_name}text"
    query = (f"SELECT * FROM table({fn}("
             f"cursor(select {col} from {table}),"
             f"1,"
             f"'{text}'));")

    _, result = heavydb.sql_execute(query)
    [result] = result
    if text in ['foo', 'bar']:
        assert result[0] > 0
    else:
        assert result[0] < 0


@pytest.mark.parametrize('col', ['t1', 't2', 't4'])
def test_getStringId_unicode(heavydb, col):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    fn = 'test_getstringid_from_unicode'
    table = f"{heavydb.base_name}text"
    query = (f"SELECT * FROM table({fn}("
             f"cursor(select {col} from {table}),"
             f"1));")

    _, result = heavydb.sql_execute(query)
    [result] = result
    assert result[0] > 0


def test_empty_input_id(heavydb):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    fn = 'test_empty_input_id'
    query = (f"SELECT * FROM table({fn}());")

    _, result = heavydb.sql_execute(query)
    result = list(result)

    assert len(result) == 2
    assert ('onedal',) in result
    assert ('mlpack',) in result


@pytest.mark.parametrize('col', ['t1', 't2', 't4'])
def test_getString(heavydb, col):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    table = f"{heavydb.base_name}text"
    _, unique = heavydb.sql_execute(f'select count(distinct {col}) from {table}')
    unique = list(unique)[0][0]

    fn = 'test_getstring'
    query = (f"SELECT * FROM table({fn}("
             f"cursor(select {col} from {table}),"
             f"{unique}));")

    _, result = heavydb.sql_execute(query)
    assert all([x[0] > 0 for x in result])


@pytest.mark.parametrize('col', ['t1', 't2', 't4'])
def test_getString_lst(heavydb, col):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    table = f"{heavydb.base_name}text"
    _, unique = heavydb.sql_execute(f'select count(distinct {col}) from {table}')
    unique = list(unique)[0][0]

    fn = 'test_getstring_lst'
    query = (f"SELECT * FROM table({fn}("
             f"cursor(select {col}, {col}, {col} from {table}),"
             f"{unique}, 1));")

    _, result = heavydb.sql_execute(query)

    if col == 't1':
        assert list(result) == [(9,), (9,), (9,), (12,), (15,)]
    else:
        assert list(result) == [(18,), (9,), (9,), (9,)]


@pytest.mark.parametrize('col', ['t1'])
def test_udf_copy_dict_encoded_string(heavydb, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB version 6.3 or newer')

    table = f"{heavydb.base_name}text"
    query = f"SELECT fn_copy({col}) from {table}"
    _, result = heavydb.sql_execute(query)

    assert list(result) == [('fun',), ('bar',), ('foo',), ('barr',), ('foooo',)]


@pytest.mark.parametrize('col', ['t1'])
@pytest.mark.parametrize('fn', ['to_text_encoding_none_1', 'to_text_encoding_none_2'])
def test_to_text_encoding_none(heavydb, fn, col):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB version 6.3 or newer')

    table = f"{heavydb.base_name}text"
    query = f"SELECT {fn}({col}) from {table}"
    _, result = heavydb.sql_execute(query)

    assert list(result) == [('fun',), ('bar',), ('foo',), ('barr',), ('foooo',)]


def test_row_function_manager(heavydb):
    if heavydb.version[:2] < (6, 3):
        pytest.skip('Requires HeavyDB version 6.3 or newer')

    @heavydb('int32(int32, RowFunctionManager)')
    def invalid_fn(a, mgr):
        return a

    with pytest.raises(TypeError) as exc:
        heavydb.register()
    assert str(exc.value) == 'RowFunctionManager ought to be the first argument'
