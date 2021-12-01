import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope="module")
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 6)):
        define(o)
        yield o


def define(omnisci):
    @omnisci("int32(Column<int64>, RowMultiplier, OutputColumn<int64>)")
    def rbc_udtf_default_sizer_1(c, sizer, o):
        o[0] = 3
        return 1


def test_python_fn_with_default_sizer(omnisci):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5403")

    table = omnisci.table_name
    fn = "rbc_udtf_default_sizer_1"
    query = f"select * from table({fn}(cursor(select i8 from {table})));"
    _, result = omnisci.sql_execute(query)

    assert list(result) == [(3,)]


typs = (
    ("1a", "1", 1011),  # ct_udtf_default_sizer1__cpu_1
    ("1a", "1r", 1011),  # ct_udtf_default_sizer1__cpu_1
    ("1a", "1R", 1041),  # ct_udtf_default_sizer1__cpu_1
    ("1b", "11", 1013),  # ct_udtf_default_sizer1__cpu_3
    ("1b", "11r", 1013),  # ct_udtf_default_sizer1__cpu_3
    ("1b", "11R", 1046),  # ct_udtf_default_sizer1__cpu_3
    ("1c", "31a", 1342),  # ct_udtf_default_sizer1__cpu_8
    ("1c", "3r1a", 1342),  # ct_udtf_default_sizer1__cpu_8
    ("1c", "3R1a", 1372),  # ct_udtf_default_sizer1__cpu_8
    ("1d", "b1", 1241),  # ct_udtf_default_sizer1__cpu_10
    ("1d", "rb1", 1241),  # ct_udtf_default_sizer1__cpu_10
    ("1d", "Rb1", 1271),  # ct_udtf_default_sizer1__cpu_10
    ("2a", "1a", 3409),  # ct_udtf_default_sizer2__cpu_2
    ("2a", "1ar", 3409),  # ct_udtf_default_sizer2__cpu_2
    ("2a", "1aR", 3412),  # ct_udtf_default_sizer2__cpu_2
    ("2b", "11", 1003),  # ct_udtf_default_sizer2__cpu_4
    ("2b", "1r1", 1003),  # ct_udtf_default_sizer2__cpu_4
    ("2b", "1R1", 1006),  # ct_udtf_default_sizer2__cpu_4
    ("2c", "b1", 2552),  # ct_udtf_default_sizer2__cpu_11
    ("2c", "br1", 2552),  # ct_udtf_default_sizer2__cpu_11
    ("2c", "bR1", 2555),  # ct_udtf_default_sizer2__cpu_11
    ("3a", "1a", 1429),  # ct_udtf_default_sizer3__cpu_5
    ("3a", "1ra", 1429),  # ct_udtf_default_sizer3__cpu_5
    ("3a", "1Ra", 1729),  # ct_udtf_default_sizer3__cpu_5
    ("3b", "1a1", 1430),  # ct_udtf_default_sizer3__cpu_6
    ("3b", "1a1r", 1430),  # ct_udtf_default_sizer3__cpu_6
    ("3b", "1a1R", 1730),  # ct_udtf_default_sizer3__cpu_6
    ("4a", "11a", 1340),  # ct_udtf_default_sizer4__cpu_7
    ("4a", "1r1a", 1340),  # ct_udtf_default_sizer4__cpu_7
    ("4a", "1R1a", 1370),  # ct_udtf_default_sizer4__cpu_7
    ("4b", "1b", 1240),  # ct_udtf_default_sizer4__cpu_9
    ("4b", "r1b", 1240),  # ct_udtf_default_sizer4__cpu_9
    ("4b", "R1b", 1267),  # ct_udtf_default_sizer4__cpu_9
)


@pytest.mark.parametrize("suffix,kind,expected", typs)
def test_default_sizer(omnisci, suffix, kind, expected):
    omnisci.require_version((5, 7), "Requires omniscidb-internal PR 5403")

    codes = {
        "1": "i4",
        "2": "i4, i4",
        "3": "i4, i4, i4",
        "a": "cast(231 as INT)",
        "b": "132",
        "r": "1",  # sizer
        "R": "4",  # another sizer
    }

    table = omnisci.table_name
    fn = f"ct_udtf_default_sizer{suffix}"
    query = f"select * from table({fn}("
    for i, n in enumerate(kind):
        cols = codes[n]
        if n.isdigit():
            query += f"cursor(select {cols} from {table})"
        else:
            query += f"{cols}"
        query += ", " if i + 1 < len(kind) else ""
    query += "));"

    _, result = omnisci.sql_execute(query)
    result = list(result)

    assert result == [(expected,)], (result, query, kind)
