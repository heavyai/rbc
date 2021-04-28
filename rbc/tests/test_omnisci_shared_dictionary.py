import os
import pytest
from rbc.tests import omnisci_fixture, skip_on_ci


@pytest.fixture(scope="module")
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 7)):
        define(o)
        yield o


def define(omnisci):
    @omnisci(
        "int32(Column<TextEncodingDict32>, RowMultiplier, OutputColumn<TextEncodingDict32>)"
    )
    def test_shared_dict(x, m, y):
        sz = len(x)
        for i in range(sz):
            y[i] = 1000 * x.get_dict_id() + x[i]
        return m * sz

    @omnisci(
        "int32(Column<T>, RowMultiplier, OutputColumn<bool>)",
        T=["TextEncodingDict32", "int32"],
    )
    def test_shared_dict_is_dict_encoded(x, m, y):
        sz = len(x)
        for i in range(sz):
            y[i] = x.is_dict_encoded()
        return m * sz


@pytest.fixture(scope="function")
def create_columns(omnisci):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    for size in (8, 16, 32):
        table_name = f"{os.path.splitext(os.path.basename(__file__))[0]}_{size}"
        base = f"base_{size}"
        derived = f"derived_{size}"

        omnisci.sql_execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {base} TEXT ENCODING DICT({size}),
                {derived} TEXT,
                SHARED DICTIONARY ({derived}) REFERENCES {table_name}({base})
            );
        """
        )

        data = {
            base: ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"],
            derived: ["world", "bar", "hello", "foo", "baz", "hello", "foo"],
        }

        omnisci.load_table_columnar(table_name, **data)

    yield omnisci

    for size in (8, 16, 32):
        table_name = f"{os.path.splitext(os.path.basename(__file__))[0]}_{size}"
        omnisci.sql_execute(f"DROP TABLE IF EXISTS {table_name}")


@skip_on_ci
@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (8, 16, 32))
def test_table_function_shared_dict(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict"
    table = f"{os.path.splitext(os.path.basename(__file__))[0]}_{size}"

    base = f"base_{size}"

    _, expected = omnisci.sql_execute(
        f"SELECT key_for_string(base_{size}) FROM {table};"
    )

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1));"
    _, result = omnisci.sql_execute(query)

    assert list(expected) == [(r[0] % 1000,) for r in list(result)]


@skip_on_ci
@pytest.mark.usefixtures("create_columns")
@pytest.mark.parametrize("size", (32,))
def test_table_function_is_shared_dict(omnisci, size):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_is_dict_encoded"
    table = f"{os.path.splitext(os.path.basename(__file__))[0]}_{size}"
    base = f"base_{size}"

    query = f"SELECT * FROM table({fn}(cursor(SELECT {base} FROM {table}), 1));"
    _, result = omnisci.sql_execute(query)

    assert all(list(map(lambda x: x[0], result)))


@skip_on_ci
def test_table_function_is_not_shared_dict(omnisci):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5492")

    fn = "test_shared_dict_is_dict_encoded"
    query = (
        f"SELECT * FROM table({fn}(cursor(SELECT i4 FROM {omnisci.table_name}), 1));"
    )
    _, result = omnisci.sql_execute(query)

    assert all(list(map(lambda x: x[0], result))) == False  # noqa: E712
