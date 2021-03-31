import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope="module")
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 5, 5)):
        define(o)
        yield o


def define(omnisci):
    @omnisci("int32(Column<int64>, RowMultiplier, OutputColumn<int64>)")
    def udtf_default_sizer_1(c, sizer, o):
        o[0] = 3
        return 1


def test_python_fn_with_default_sizer(omnisci):

    table = omnisci.table_name
    fn = "udtf_default_sizer_1"
    query = f"select * from table({fn}(cursor(select i8 from {table})));"
    _, result = omnisci.sql_execute(query)

    assert list(result) == [(3,)]


typs = (
    ("1", "1"),  # ct_udtf_default_sizer1__cpu_1
    ("1", "1r"),  # ct_udtf_default_sizer1__cpu_1
    ("2", "1a"),  # ct_udtf_default_sizer2__cpu_1
    ("2", "1ar"),  # ct_udtf_default_sizer2__cpu_1
    ("3", "11"),  # ct_udtf_default_sizer3__cpu_1
    ("3", "11r"),  # ct_udtf_default_sizer3__cpu_1
    ("4", "11"),  # ct_udtf_default_sizer4__cpu_1
    ("4", "1r1"),  # ct_udtf_default_sizer4__cpu_1
    ("5", "1a"),  # ct_udtf_default_sizer5__cpu_1
    ("5", "1ra"),  # ct_udtf_default_sizer5__cpu_1
    ("6", "1a1"),  # ct_udtf_default_sizer6__cpu_1
    ("6", "1a1r"),  # ct_udtf_default_sizer6__cpu_1
    ("7", "11a"),  # ct_udtf_default_sizer7__cpu_1
    ("7", "1r1a"),  # ct_udtf_default_sizer7__cpu_1
    ("8", "31a"),  # ct_udtf_default_sizer8__cpu_1
    ("8", "3r1a"),  # ct_udtf_default_sizer8__cpu_1
)


@pytest.mark.parametrize("suffix,kind", typs)
def test_default_sizer(omnisci, suffix, kind):
    omnisci.require_version((5, 5, 5), "Requires omniscidb-internal PR 5403")

    codes = {
        "1": "i4",
        "2": "i4, i4",
        "3": "i4, i4, i4",
        "a": "cast(231 as INT)",
        "b": "132",
        "r": "1",
    }

    table = omnisci.table_name
    fn = f"ct_udtf_default_sizer{suffix}"
    query = f"select * from table({fn}("
    for i, n in enumerate(kind):
        cols = codes[n]
        if str.isdigit(n):
            query += f"cursor(select {cols} from {table})"
        else:
            query += f"{cols}"
        query += ", " if i + 1 < len(kind) else ""
    query += "));"

    _, result = omnisci.sql_execute(query)
    result = list(result)

    r = sum(map(lambda x: int(x) if str.isdigit(x) else ord(x), kind))
    if "r" in kind:
        r -= ord("r")
    assert result == [(1000 + r,)], (result, query)
