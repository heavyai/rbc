import os
import pytest
import itertools
from rbc.tests import omnisci_fixture, skip_on_ci


@pytest.fixture(scope="module")
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 7)):
        define(o)
        o.base_name = os.path.splitext(os.path.basename(__file__))[0]
        yield o


def define(omnisci):
    @omnisci("int32(Column<T>, RowMultiplier, OutputColumn<T>)",
             T=["TextEncodingDict"])
    def test_shared_dict_copy(x, m, y):
        sz = len(x)
        for i in range(sz):
            y[i] = x[i]
        return m * sz

    @omnisci("int32(Column<T>, Column<T>, int32_t, RowMultiplier, OutputColumn<T>)",
             T=["TextEncodingDict"])
    def test_shared_dict_copy2(x, y, s, m, z):
        sz = len(x)
        for i in range(sz):
            if s == 0:
                z[i] = x[i]
            else:
                z[i] = y[i]
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

    @omnisci("int32(Column<T>, RowMultiplier, OutputColumn<bool>)",
             T=["TextEncodingDict", "int32"])
    def test_shared_dict_is_dict_encoded(x, m, y):
        sz = len(x)
        for i in range(sz):
            y[i] = x.is_dict_encoded()
        return m * sz


@pytest.fixture(scope="function")
def create_columns(omnisci):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    for size in (8, 16, 32):
        table_name = f"{omnisci.base_name}_{size}"
        base = f"base_{size}"
        other = f"other_{size}"
        derived = f"derived_{size}"

        omnisci.sql_execute(f"DROP TABLE IF EXISTS {table_name};")

        omnisci.sql_execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {base} TEXT ENCODING DICT({size}),
                {other} TEXT ENCODING DICT({size}),
                {derived} TEXT,
                SHARED DICTIONARY ({derived}) REFERENCES {table_name}({base})
            );"""
        )

        data = {
            base: ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"],
            other: ["a1", "b2", "c3", "d4", "e5", "f6", "g7"],
            derived: ["world", "bar", "hello", "foo", "baz", "hello", "foo"],
        }

        omnisci.load_table_columnar(table_name, **data)

    yield omnisci

    for size in (8, 16, 32):
        table_name = f"{omnisci.base_name}_{size}"
        omnisci.sql_execute(f"DROP TABLE IF EXISTS {table_name}")


@skip_on_ci
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


@skip_on_ci
@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32))
@pytest.mark.parametrize("select", (0, 1))
def test_text_encoding_shared_dict2(omnisci, select, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    if select == 1:
        pytest.xfail(
            "Test will fail due to inability of "
            "assigning the dictionary id at runtime")

    fn = "test_shared_dict_copy2"
    table = f"{omnisci.base_name}_{size}"

    base = f"base_{size}"
    other = f"other_{size}"

    query = (
        f"SELECT * FROM table({fn}("
        f"cursor(SELECT {other} FROM {table}), "
        f"cursor(SELECT {base} FROM {table}), "
        f"cast({select} as INT), "
        "1))")
    _, result = omnisci.sql_execute(query)

    assert list(result) == [("a1",), ("b2",), ("c3",), ("d4",), ("e5",), ("f6",), ("g7",)]


@skip_on_ci
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


@skip_on_ci
@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
def test_text_encoding_is_shared_dict(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_is_dict_encoded"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1));"
    _, result = omnisci.sql_execute(query)

    assert all(list(map(lambda x: x[0], result)))


@skip_on_ci
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


@skip_on_ci
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


@skip_on_ci
def test_text_encoding_is_not_shared_dict(omnisci):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_is_dict_encoded"
    query = (
        f"SELECT * FROM table({fn}(cursor(SELECT i4 FROM {omnisci.table_name}), 1));"
    )
    _, result = omnisci.sql_execute(query)

    assert all(list(map(lambda x: x[0], result))) == False  # noqa: E712
