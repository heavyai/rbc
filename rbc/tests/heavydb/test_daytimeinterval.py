import pytest
from rbc.tests import heavydb_fixture
from rbc.errors import HeavyDBServerError

rbc_heavydb = pytest.importorskip("rbc.heavydb")
available_version, reason = rbc_heavydb.is_available()


@pytest.fixture(scope="module")
def heavydb():
    for o in heavydb_fixture(
        globals(), debug=False, load_test_data=False, minimal_version=(6, 2)
    ):
        try:
            o.sql_execute('select * from table(generate_series(0, 10, 1));')
        except Exception as e:
            msg = "Undefined function call 'generate_series' in SQL statement"
            if msg in str(e):
                pytest.skip('test requires server to be compiled with SYSTEM_TFS '
                            f'enabled ({e}):')
            else:
                raise
        define(o)
        yield o


def define(heavydb):
    @heavydb(
        "int32_t(TableFunctionManager, T start, T stop, K step, OutputColumn<T> generate_series)",
        T=["Timestamp"],
        K=["YearMonthTimeInterval", "DayTimeInterval"],
        devices=['cpu']
    )
    def rbc_generate_series(mgr, start, stop, step, series_output):
        if step.timeval == 0:
            return mgr.error_message("Timestamp division by zero")

        num_rows = step.numStepsBetween(start, stop) + 1
        if num_rows <= 0:
            mgr.set_output_row_size(0)
            return 0

        mgr.set_output_row_size(num_rows)

        for idx in range(num_rows):
            series_output[idx] = start + (step * idx)
        return num_rows


inputs = [
    (
        # 3-arg version, test with step of 1 second
        # Test non-named and named arg versions
        "TIMESTAMP(9) '1970-01-01 00:00:03.000000000'",
        "TIMESTAMP(9) '1970-01-01 00:00:07.000000000'",
        "INTERVAL '1' second",
        "ASC",
    ),
    (
        # Negative step and stop > start should return 0 rows
        "TIMESTAMP(9) '1970-01-01 00:00:02.000000000'",
        "TIMESTAMP(9) '1970-01-01 00:00:05.000000000'",
        "INTERVAL '-1' second",
        "ASC",
    ),
    (
        # Negative step and stop > start should return 0 rows
        "TIMESTAMP(9) '1970-01-01 00:00:05.000000000'",
        "TIMESTAMP(9) '1970-01-01 00:00:02.000000000'",
        "INTERVAL '1' second",
        "ASC",
    ),
    (
        # Negative step and stop == start should return 1 row (start)
        "TIMESTAMP(9) '1970-01-01 00:00:02.000000000'",
        "TIMESTAMP(9) '1970-01-01 00:00:02.000000000'",
        "INTERVAL '-1' second",
        "ASC",
    ),
    (
        # Positive step and stop == start should return 1 row (start)
        "TIMESTAMP(9) '1970-01-01 00:00:02.000000000'",
        "TIMESTAMP(9) '1970-01-01 00:00:02.000000000'",
        "INTERVAL '1' second",
        "ASC",
    ),
    (
        # 3-arg version - test step of 2 minutes
        # Test non-namned and named arg versions
        "TIMESTAMP(9) '1970-01-01 00:01:00.000000000'",
        "TIMESTAMP(9) '1970-01-01 00:10:00.000000000'",
        "INTERVAL '2' minute",
        "ASC",
    ),
    (
        # Series should be inclusive of stop value
        "TIMESTAMP(9) '2000-04-04 01:00:00.000000000'",
        "TIMESTAMP(9) '2000-04-04 09:00:00.000000000'",
        "INTERVAL '2' hour",
        "ASC",
    ),
    (
        # Negative hour step
        "TIMESTAMP(9) '1999-09-04 09:00:00.000000000'",
        "TIMESTAMP(9) '1999-09-04 01:00:00.000000000'",
        "INTERVAL '-1' hour",
        "DESC",
    ),
    (
        # 3-arg version - test month time intervals
        # Test non-namned and named arg versions
        "TIMESTAMP(9) '1970-01-01 00:00:00.000000000'",
        "TIMESTAMP(9) '1970-09-01 00:00:00.000000000'",
        "INTERVAL '1' month",
        "ASC",
    ),
    (
        # Negative month step
        "TIMESTAMP(9) '1999-09-04 09:00:00.000000000'",
        "TIMESTAMP(9) '1999-01-04 09:00:00.000000000'",
        "INTERVAL '-1' MONTH",
        "DESC",
    ),
    (
        # 3-arg version - test year time intervals
        # Test non-namned and named arg versions
        "TIMESTAMP(9) '1970-01-01 00:00:00.000000000'",
        "TIMESTAMP(9) '1979-01-01 00:00:00.000000000'",
        "INTERVAL '2' year",
        "ASC",
    ),
    (
        # Negative year step
        "TIMESTAMP(9) '1999-09-04 09:00:00.000000000'",
        "TIMESTAMP(9) '1991-09-04 09:00:00.000000000'",
        "INTERVAL '-1' YEAR",
        "DESC",
    ),
]


@pytest.mark.parametrize("start, stop, step, order", inputs)
def test_generate_time_series(heavydb, start, stop, step, order):

    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    RBC_FUNC = "rbc_generate_series"
    HEAVYDB_FUNC = "generate_series"

    non_named_arg_query = (
        "SELECT generate_series FROM TABLE({0}("
        f"{start},"
        f"{stop},"
        f"{step}))"
        f"ORDER BY generate_series {order};"
    )

    named_arg_query = (
        "SELECT generate_series FROM TABLE({0}("
        f"series_start=>{start},"
        f"series_stop=>{stop},"
        f"series_step=>{step}))"
        f"ORDER BY generate_series {order};"
    )

    for query in (non_named_arg_query, named_arg_query):
        _, rbc_result = heavydb.sql_execute(non_named_arg_query.format(RBC_FUNC))
        _, heavy_result = heavydb.sql_execute(non_named_arg_query.format(HEAVYDB_FUNC))
        assert list(rbc_result) == list(heavy_result)


invalid_inputs = [
    (
        # Step of 0 days is not permitted
        "TIMESTAMP(9) '1970-01-01 00:00:00.000000010'",
        "TIMESTAMP(9) '1970-01-01 00:00:00.000000020'",
        "INTERVAL '0' day",
        "Timestamp division by zero",
    ),
    (
        # Step of 0 months is not permitted
        "TIMESTAMP(9) '1970-01-01 00:00:00.000000010'",
        "TIMESTAMP(9) '1970-01-01 00:00:00.000000020'",
        "INTERVAL '0' month",
        "Timestamp division by zero",
    ),
]


@pytest.mark.parametrize("start, stop, step, error_msg", invalid_inputs)
def test_generate_series_invalid_inputs(heavydb, start, stop, step, error_msg):

    if heavydb.version[:2] < (6, 2):
        pytest.skip('Requires HeavyDB version 6.2 or newer')

    query = (
        "SELECT generate_series FROM TABLE(rbc_generate_series("
        f"{start},"
        f"{stop},"
        f"{step}));"
    )
    with pytest.raises(HeavyDBServerError) as exc:
        _, result = heavydb.sql_execute(query)

    assert exc.match(error_msg)
