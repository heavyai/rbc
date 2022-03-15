import os
import pytest
import itertools
from rbc.tests import omnisci_fixture


@pytest.fixture(scope="module")
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 7)):
        define(o)
        o.base_name = os.path.splitext(os.path.basename(__file__))[0]
        yield o


def define(omnisci):
    @omnisci("int32(Column<T>, RowMultiplier, OutputColumn<T> | input_id=args<0>)",
             T=["TextEncodingDict"])
    def test_shared_dict_copy(x, m, y):
        sz = len(x)
        for i in range(sz):
            y[i] = x[i]
        return m * sz

    @omnisci("int32(Column<T>, Column<T>, Column<T>, RowMultiplier, "
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

    @omnisci("int32(ColumnList<TextEncodingDict>, RowMultiplier, OutputColumn<int32_t>)",
             devices=['cpu'])
    def test_copy_column_list(lst, m, y):
        for j in range(len(y)):
            y[j] = 0

        for i in range(lst.ncols):
            col = lst[i]
            for j in range(lst.nrows):
                y[j] += col[j]
        return lst.nrows

    @omnisci("int32(ColumnList<T>, Column<int32_t>, ColumnList<T>, RowMultiplier, "
             "OutputColumn<T> | input_id=args<0, 2>, "
             "OutputColumn<T> | input_id=args<2, 0>, "
             "OutputColumn<T> | input_id=args<2, 1>)",
             devices=['cpu'], T=["TextEncodingDict"])
    def test_copy_column_list2(lst1, col, lst2, m, a, b, c):
        for j in range(lst1.nrows):
            a[j] = lst1[2][j]
            b[j] = lst2[0][j]
            c[j] = lst2[1][j]
        return lst1.nrows

    @omnisci("int32(ColumnList<T>, Column<U>, RowMultiplier, OutputColumn<U> | input_id=args<1>)",
             devices=['cpu'], T=["int32_t"], U=["TextEncodingDict"])
    def test_copy_column_list3(lst, col, m, y):
        sz = len(col)
        for j in range(sz):
            y[j] = col[j]
        return m * sz


@pytest.fixture(scope="function")
def create_columns(omnisci):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    for size in (8, 16, 32):
        table_name = f"{omnisci.base_name}_{size}"
        base = f"base_{size}"
        other = f"other_{size}"
        derived = f"derived_{size}"
        another = f"another_{size}"

        omnisci.sql_execute(f"DROP TABLE IF EXISTS {table_name};")

        omnisci.sql_execute(
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

        omnisci.load_table_columnar(table_name, **data)

    yield omnisci

    for size in (8, 16, 32):
        table_name = f"{omnisci.base_name}_{size}"
        omnisci.sql_execute(f"DROP TABLE IF EXISTS {table_name}")


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32))
def test_text_encoding_shared_dict(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1))"
    _, result = omnisci.sql_execute(query)

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
def test_text_encoding_shared_dict2(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5719")

    fn = "test_shared_dict_copy2"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"
    another = f"another_{size}"

    ans_query = (f"SELECT {other}, {base}, {another} FROM {table};")
    _, ans = omnisci.sql_execute(ans_query)

    query = (f"SELECT * FROM table({fn}("
             f"cursor(SELECT {base} FROM {table}), "
             f"cursor(SELECT {other} FROM {table}), "
             f"cursor(SELECT {another} FROM {table}), "
             "1))")
    _, result = omnisci.sql_execute(query)

    assert list(ans) == list(result)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("col_name", ("base", "other", "another"))
@pytest.mark.parametrize("size", (8, 16, 32))
def test_text_encoding_shared_dict3(omnisci, col_name, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{omnisci.base_name}_{size}"
    col = f"{col_name}_{size}"

    ans_query = (f"SELECT {col} FROM {table};")
    _, ans = omnisci.sql_execute(ans_query)

    query = f"SELECT * FROM table({fn}(cursor(SELECT {col} FROM {table}), 1))"
    _, result = omnisci.sql_execute(query)

    assert list(result) == list(ans)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
@pytest.mark.parametrize("num_column_list", (1, 2, 3))
def test_text_encoding_column_list(omnisci, size, num_column_list):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_copy_column_list"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"
    cols = ', '.join([base] * num_column_list)

    _, expected = omnisci.sql_execute(
        f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1));")

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {cols} FROM {table}), "
        "1));")
    _, result = omnisci.sql_execute(query)

    assert list(map(lambda x: x[0] * num_column_list, expected)) == \
        list(itertools.chain.from_iterable(result))


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (32,))
def test_text_encoding_column_list2(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5719")

    fn = "test_copy_column_list2"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"
    another = f"another_{size}"
    cols = f"{base}, {other}, {another}"

    ans_query = (f"SELECT {another}, {base}, {other} FROM {table};")
    _, ans = omnisci.sql_execute(ans_query)

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {cols} FROM {table}), "
        f"cursor(SELECT i4 FROM {omnisci.table_name}), "
        f"cursor(SELECT {cols} FROM {table}), "
        "1));")
    _, result = omnisci.sql_execute(query)

    assert list(ans) == list(result)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (32,))
@pytest.mark.parametrize("num_cols", (1, 2, 3, 4))
def test_text_encoding_column_list3(omnisci, size, num_cols):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5719")

    fn = "test_copy_column_list3"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"
    cols = ", ".join(["i4"] * num_cols)

    _, ans = omnisci.sql_execute(f"SELECT {base} FROM {table} LIMIT 5;")

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {cols} FROM {omnisci.table_name}), "
        f"cursor(SELECT {base} FROM {table}), "
        "1));")
    _, result = omnisci.sql_execute(query)

    assert list(ans) == list(result)


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_text_encoding_count(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT COUNT(out0) FROM table({fn}(cursor(SELECT {base} FROM {table}), 1))"
    _, result = omnisci.sql_execute(query)

    assert list(result) == [(7,)]


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_text_encoding_order_by(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_copy"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"

    query = (
        f"SELECT out0 FROM table({fn}(cursor(SELECT {base} FROM {table}), 1))"
        " ORDER BY out0;")
    _, result = omnisci.sql_execute(query)

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
def test_ct_binding_dict_encoded1(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = 'ct_binding_dict_encoded1'
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table})));"
    _, result = omnisci.sql_execute(query)
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
def test_ct_binding_dict_encoded23(omnisci, size, fn_suffix):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5719")

    fn = f"ct_binding_dict_encoded{fn_suffix}"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base}, {other} FROM {table})));"
    _, result = omnisci.sql_execute(query)

    out0 = ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"]
    out1 = ["a1", "b2", "c3", "d4", "e5", "f6", "g7"]

    if fn_suffix == "2":
        assert list(result) == list(zip(out0, out1))
    else:
        assert list(result) == list(zip(out1, out0))


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
@pytest.mark.parametrize("fn_suffix", ("4", "5", "6"))
def test_ct_binding_dict_encoded45(omnisci, size, fn_suffix):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5719")

    fn = f"ct_binding_dict_encoded{fn_suffix}"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"
    other = f"other_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {other}, {base}, {other} FROM {table})));"
    _, result = omnisci.sql_execute(query)

    out4 = ["a1", "b2", "c3", "d4", "e5", "f6", "g7"]
    out5 = ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"]

    if fn_suffix == "4":
        assert list(sum(result, ())) == out4
    elif fn_suffix == "5":
        assert list(sum(result, ())) == out5
    else:
        assert list(result) == list(zip(out4, out5))
