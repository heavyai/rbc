import pytest
import numpy as np
from rbc.tests import heavydb_fixture
from rbc.externals.heavydb import set_output_row_size
from rbc.heavydb import Timestamp

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), debug=False,
                             suffices=['timestamp'], minimal_version=(6, 0)):
        define(o)
        yield o


def define(heavydb):

    @heavydb('int32_t(Column<int64_t>, RowMultiplier, OutputColumn<Timestamp>)',
             devices=['cpu', 'gpu'])
    def int_ctor(x, m, y):
        for i in range(len(x)):
            y[i] = Timestamp(x[i])
        return len(x)

    @heavydb('int32_t(Column<int64_t>, RowMultiplier, OutputColumn<Timestamp>)',
             devices=['cpu', 'gpu'])
    def text_ctor(x, m, y):
        for i in range(len(x)):
            if i == 0:
                y[i] = Timestamp("2021-01-01 01:01:01.001001001")
            elif i == 1:
                y[i] = Timestamp("2022-02-02 02:02:02.002002002")
            else:
                y[i] = Timestamp("2023-03-03 03:03:03.003003003")
        return len(x)

    @heavydb('int32_t(Column<Timestamp>, RowMultiplier, OutputColumn<int64_t>)',
             devices=['cpu', 'gpu'])
    def timestamp_null_check(x, m, y):
        for i in range(len(x)):
            y[i] = x.is_null(i)
        return len(x)

    @heavydb("int32_t(Column<Timestamp>, TextEncodingNone, OutputColumn<int64_t>)",
             devices=['cpu'])
    def timestamp_get(x, op, y):
        set_output_row_size(len(x))
        for i in range(len(x)):
            if op == 'getYear':
                y[i] = x[i].getYear()
            elif op == 'getMonth':
                y[i] = x[i].getMonth()
            elif op == 'getDay':
                y[i] = x[i].getDay()
            elif op == 'getHours':
                y[i] = x[i].getHours()
            elif op == 'getMinutes':
                y[i] = x[i].getMinutes()
            elif op == 'getSeconds':
                y[i] = x[i].getSeconds()
            elif op == 'getMilliseconds':
                y[i] = x[i].getMilliseconds()
            elif op == 'getMicroseconds':
                y[i] = x[i].getMicroseconds()
            elif op == 'getNanoseconds':
                y[i] = x[i].getNanoseconds()
            else:
                y[i] = -1
        return len(x)

    @heavydb("int32(Column<Timestamp>, TextEncodingNone, OutputColumn<Timestamp>)",
             devices=['cpu'])
    def timestamp_truncate_cpu(x, op, y):
        set_output_row_size(len(x))
        for i in range(len(x)):
            if op == 'truncateToYear':
                y[i] = x[i].truncateToYear()
            elif op == 'truncateToMonth':
                y[i] = x[i].truncateToMonth()
            elif op == 'truncateToDay':
                y[i] = x[i].truncateToDay()
            elif op == 'truncateToHours':
                y[i] = x[i].truncateToHours()
            elif op == 'truncateToMinutes':
                y[i] = x[i].truncateToMinutes()
            elif op == 'truncateToSeconds':
                y[i] = x[i].truncateToSeconds()
            elif op == 'truncateToMilliseconds':
                y[i] = x[i].truncateToMilliseconds()
            elif op == 'truncateToMicroseconds':
                y[i] = x[i].truncateToMicroseconds()
        return len(x)

    @heavydb('int32_t(Column<Timestamp>, Column<Timestamp>, TextEncodingNone, OutputColumn<int32_t>)',  # noqa: E501
             devices=['cpu'])
    def timestamp_operator_cpu(a, b, op, y):
        set_output_row_size(len(a))
        for i in range(len(a)):
            if op == 'eq':
                y[i] = (a[i] == b[i])
            elif op == 'ne':
                y[i] = (a[i] != b[i])
            elif op == 'lt':
                y[i] = (a[i] < b[i])
            elif op == 'le':
                y[i] = (a[i] <= b[i])
            elif op == 'gt':
                y[i] = (a[i] > b[i])
            elif op == 'ge':
                y[i] = (a[i] >= b[i])
            else:
                y[i] = 127
        return len(a)

    @heavydb('int32_t(Column<Timestamp>, Column<Timestamp>, TextEncodingNone, OutputColumn<Timestamp>)',  # noqa: E501
             devices=['cpu'])
    def timestamp_operator_2_cpu(a, b, op, y):
        set_output_row_size(len(a))
        for i in range(len(a)):
            if op == 'floordiv':
                y[i] = (a[i] // b[i])
            elif op == 'truediv':
                y[i] = (a[i] / b[i])
            elif op == 'add':
                y[i] = (a[i] + b[i])
            elif op == 'sub':
                y[i] = (a[i] - b[i])
            elif op == 'mul':
                y[i] = (a[i] * b[i])
            else:
                y[i] = Timestamp(127)
        return len(a)

    if heavydb.has_cuda:
        @heavydb('int32(Column<Timestamp>, RowMultiplier, int32_t, OutputColumn<Timestamp>)',
                 devices=['gpu'])
        def timestamp_truncate_gpu(x, m, op, y):
            for i in range(len(x)):
                if op == 0:
                    y[i] = x[i].truncateToSeconds()
                elif op == 1:
                    y[i] = x[i].truncateToMilliseconds()
                elif op == 2:
                    y[i] = x[i].truncateToMicroseconds()
            return len(x)

        @heavydb('int32_t(Column<Timestamp>, Column<Timestamp>, RowMultiplier, int32_t, OutputColumn<int32_t>)',  # noqa: E501
                 devices=['gpu'])
        def timestamp_operator_gpu(a, b, m, op, y):
            for i in range(len(a)):
                if op == 0:
                    y[i] = (a[i] == b[i])
                elif op == 1:
                    y[i] = (a[i] != b[i])
                elif op == 2:
                    y[i] = (a[i] < b[i])
                elif op == 3:
                    y[i] = (a[i] <= b[i])
                elif op == 4:
                    y[i] = (a[i] > b[i])
                elif op == 5:
                    y[i] = (a[i] >= b[i])
                else:
                    y[i] = 127
            return len(a)

        @heavydb('int32_t(Column<Timestamp>, Column<Timestamp>, RowMultiplier, int32_t, OutputColumn<Timestamp>)',  # noqa: E501
                 devices=['gpu'])
        def timestamp_operator_2_gpu(a, b, m, op, y):
            for i in range(len(a)):
                if op == 0:
                    y[i] = (a[i] + b[i])
                elif op == 1:
                    y[i] = (a[i] - b[i])
                elif op == 2:
                    y[i] = (a[i] * b[i])
                elif op == 3:
                    y[i] = (a[i] // b[i])
                elif op == 4:
                    y[i] = (a[i] / b[i])
                else:
                    y[i] = Timestamp(127)
            return len(a)


@pytest.mark.parametrize("method", ["year", "month", "day", "hours", "minutes", "seconds",
                                    "milliseconds", "microseconds"])
@pytest.mark.parametrize("col", ['t9'])
def test_getMethods(heavydb, col, method):

    table = f"{heavydb.table_name}timestamp"
    query = (f"select * from table(timestamp_get(cursor("
             f"select {col} from {table}), 'get{method.capitalize()}'));")
    _, result = heavydb.sql_execute(query)

    if method == "year":
        assert list(result) == [(1971,), (1972,), (1973,)]
    elif method == "month":
        assert list(result) == [(1,), (2,), (3,)]
    elif method == "day":
        assert list(result) == [(1,), (2,), (3,)]
    elif method == "hours":
        assert list(result) == [(1,), (2,), (3,)]
    elif method == "minutes":
        assert list(result) == [(1,), (2,), (3,)]
    elif method == "seconds":
        assert list(result) == [(1,), (2,), (3,)]
    elif method == "milliseconds":
        assert list(result) == [(1001,), (2002,), (3003,)]
    elif method == "microseconds":
        assert list(result) == [(1001001,), (2002002,), (3003003,)]
    elif method == "nanoseconds":
        assert list(result) == [(1001001001,), (2002002002,), (3003003003,)]
    else:
        assert 0


@pytest.mark.parametrize('device', ['cpu', 'gpu'])
@pytest.mark.parametrize("method", ["year", "month", "day", "hours", "minutes", "seconds",
                                    "milliseconds", "microseconds"])
@pytest.mark.parametrize("col", ['t9'])
def test_truncateMethods(heavydb, device, col, method):

    if device == 'gpu' and not heavydb.has_cuda:
        pytest.skip('test requires CUDA-enabled heavydb server')

    table = f"{heavydb.table_name}timestamp"
    if device == 'cpu':
        query = (f"select * from table(timestamp_truncate_{device}("
                 f"cursor(select {col} from {table}), 'truncateTo{method.capitalize()}'));")
    else:
        methods = ['seconds', 'milliseconds', 'microseconds']
        if method not in methods:
            pytest.skip(f"truncateTo{method.capitalize()}() not implemented for GPU")
        idx = methods.index(method)
        query = (f"select * from table(timestamp_truncate_{device}("
                 f"cursor(select {col} from {table}), 1, {idx}));")
    _, result = heavydb.sql_execute(query)

    if method == "year":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 00:00:00.000000000"),
                    np.datetime64("1972-01-01 00:00:00.000000000"),
                    np.datetime64("1973-01-01 00:00:00.000000000")]
        assert result == expected
    elif method == "month":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 00:00:00.000000000"),
                    np.datetime64("1972-02-01 00:00:00.000000000"),
                    np.datetime64("1973-03-01 00:00:00.000000000")]
        assert result == expected
    elif method == "day":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 00:00:00.000000000"),
                    np.datetime64("1972-02-02 00:00:00.000000000"),
                    np.datetime64("1973-03-03 00:00:00.000000000")]
        assert result == expected
    elif method == "hours":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 01:00:00.000000000"),
                    np.datetime64("1972-02-02 02:00:00.000000000"),
                    np.datetime64("1973-03-03 03:00:00.000000000")]
        assert result == expected
    elif method == "minutes":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 01:01:00.000000000"),
                    np.datetime64("1972-02-02 02:02:00.000000000"),
                    np.datetime64("1973-03-03 03:03:00.000000000")]
        assert result == expected
    elif method == "seconds":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 01:01:01.000000000"),
                    np.datetime64("1972-02-02 02:02:02.000000000"),
                    np.datetime64("1973-03-03 03:03:03.000000000")]
        assert result == expected
    elif method == "milliseconds":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 01:01:01.001000000"),
                    np.datetime64("1972-02-02 02:02:02.002000000"),
                    np.datetime64("1973-03-03 03:03:03.003000000")]
        assert result == expected
    elif method == "microseconds":
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64("1971-01-01 01:01:01.001001000"),
                    np.datetime64("1972-02-02 02:02:02.002002000"),
                    np.datetime64("1973-03-03 03:03:03.003003000")]
        assert result == expected
    else:
        assert 0


@pytest.mark.parametrize('device', ['cpu', 'gpu'])
@pytest.mark.parametrize('op', ['eq', 'ne', 'lt', 'le', 'gt', 'ge'])
@pytest.mark.parametrize('col1', ['t9', 't9_2'])
@pytest.mark.parametrize('col2', ['t9_2', 't9'])
def test_operators(heavydb, device, col1, col2, op):

    if device == 'gpu' and not heavydb.has_cuda:
        pytest.skip('test requires CUDA-enabled heavydb server')

    table_op = {
        'eq': '=',
        'ne': '!=',
        'lt': '<',
        'le': '<=',
        'gt': '>',
        'ge': '>=',

    }

    table = f'{heavydb.table_name}timestamp'
    _, expected = heavydb.sql_execute(f'select {col1} {table_op[op]} {col2} from {table}')

    if device == 'cpu':
        query = (f"select * from table(timestamp_operator_{device}("
                 f"cursor(select {col1} from {table}),"
                 f"cursor(select {col2} from {table}),"
                 f"'{op}'))")
    else:
        idx = list(table_op.keys()).index(op)
        query = (f"select * from table(timestamp_operator_{device}("
                 f"cursor(select {col1} from {table}),"
                 f"cursor(select {col2} from {table}),"
                 f"{idx}))")
    _, result = heavydb.sql_execute(query)

    assert list(result) == list(expected)


@pytest.mark.parametrize('device', ['cpu', 'gpu'])
@pytest.mark.parametrize('op', ['add', 'sub', 'mul', 'floordiv', 'truediv'])
def test_operators2(heavydb, device, op):

    if device == 'gpu' and not heavydb.has_cuda:
        pytest.skip('test requires CUDA-enabled heavydb server')

    col1 = 't9_2'
    col2 = 't9'

    table = f'{heavydb.table_name}timestamp'
    if device == 'cpu':
        query = (f"select * from table(timestamp_operator_2_{device}("
                 f"cursor(select {col1} from {table}),"
                 f"cursor(select {col2} from {table}),"
                 f"'{op}'))")
    else:
        idx = ['add', 'sub', 'mul', 'floordiv', 'truediv'].index(op)
        query = (f"select * from table(timestamp_operator_2_{device}("
                 f"cursor(select {col1} from {table}),"
                 f"cursor(select {col2} from {table}),"
                 f"1,"
                 f"{idx}));")
    _, result = heavydb.sql_execute(query)

    if op == 'add':
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64('2022-01-01 02:02:02.002002002'),
                    np.datetime64('2024-03-05 04:04:04.004004004'),
                    np.datetime64('2026-05-03 06:06:06.006006006')]
        assert result == expected
    elif op == 'sub':
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64('2020-01-02'),
                    np.datetime64('2020-01-02'),
                    np.datetime64('2020-01-01')]
        assert result == expected
    elif op == 'mul':
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64('2185-09-18 11:54:21.428053649'),
                    np.datetime64('1732-10-05 06:40:02.933971524'),
                    np.datetime64('2139-05-27 07:08:53.212359449')]
        assert result == expected
    elif op == 'floordiv':
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64('1970-01-01 00:00:00.000000051'),
                    np.datetime64('1970-01-01 00:00:00.000000024'),
                    np.datetime64('1970-01-01 00:00:00.000000016')]
        assert result == expected
    elif op == 'truediv':
        result = list(map(lambda x: np.datetime64(x[0], 'ns'), result))
        expected = [np.datetime64('1970-01-01 00:00:00.000000051'),
                    np.datetime64('1970-01-01 00:00:00.000000024'),
                    np.datetime64('1970-01-01 00:00:00.000000016')]
        assert result == expected
    else:
        assert 0


@pytest.mark.parametrize('col', ['i8_2'])
def test_int_ctor(heavydb, col):
    table = f"{heavydb.table_name}timestamp"

    query = (f"select * from table(int_ctor("
             f"cursor(select {col} from {table}), 1))")
    _, result = heavydb.sql_execute(query)
    _, expected = heavydb.sql_execute(f"select t9_2 from {table}")

    assert list(result) == list(expected)


def test_literal_string_ctor(heavydb):
    table = f"{heavydb.table_name}timestamp"

    query = (f"select * from table(text_ctor("
             f"cursor(select i8_2 from {table})))")
    _, result = heavydb.sql_execute(query)
    _, expected = heavydb.sql_execute(f"select t9_2 from {table}")

    assert list(result) == list(expected)


@pytest.mark.parametrize('col', ['t9_null'])
def test_is_null(heavydb, col):
    table = f"{heavydb.table_name}timestamp"

    query = (f"select * from table(timestamp_null_check("
             f"cursor(select {col} from {table})))")
    _, result = heavydb.sql_execute(query)

    assert list(result) == [(0,), (1,), (0,)]
