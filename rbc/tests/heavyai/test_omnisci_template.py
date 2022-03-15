import os
import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope="module")
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 7)):
        o.base_name = os.path.splitext(os.path.basename(__file__))[0]
        yield o


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


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (32,))
def test_template_text(omnisci, size):

    if omnisci.has_cuda:
        omnisci.require_version(
            (5, 8), "Requires omniscidb-internal PR 5809")

    fn = "ct_binding_template"
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


@pytest.mark.parametrize("col", ("i4", "f4"))
def test_template_number(omnisci, col):

    if omnisci.has_cuda:
        omnisci.require_version(
            (5, 8), "Requires omniscidb-internal PR 5809")

    fn = "ct_binding_template"
    table = omnisci.table_name

    query = f"SELECT * FROM table({fn}(cursor(SELECT {col} FROM {table})));"
    _, result = omnisci.sql_execute(query)
    assert list(result) == [(0,), (1,), (2,), (3,), (4,)]


@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32,))
@pytest.mark.parametrize("out", (3,))
def test_template_columnlist_text(omnisci, size, out):

    fn = "ct_binding_columnlist"
    table = f"{omnisci.base_name}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base}, {base}, {base} FROM {table})));"
    _, result = omnisci.sql_execute(query)
    assert list(result) == [(out,)]


@pytest.mark.parametrize("col,out", zip(("i4", "f4", "i2"), (1, 2, 4)))
def test_template_columnlist_number(omnisci, col, out):

    fn = "ct_binding_columnlist"
    table = omnisci.table_name

    query = f"SELECT * FROM table({fn}(cursor(SELECT {col}, {col}, {col} FROM {table})));"
    _, result = omnisci.sql_execute(query)
    assert list(result) == [(out,)]


@pytest.mark.parametrize("col,out", zip(("i4", "f4"), (10, 20)))
def test_template_column_number(omnisci, col, out):
    omnisci.require_version((5, 8), "Requires omniscidb-internal PR #5770")

    fn = "ct_binding_column"
    table = omnisci.table_name

    query = f"SELECT * FROM table({fn}(cursor(SELECT {col} FROM {table})));"
    _, result = omnisci.sql_execute(query)
    assert list(result) == [(out,)]
