
import os
from collections import defaultdict
import pytest
import numpy as np

rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
if available_version and available_version < (5, 4):
    reason = ('New-style UDTFs (with Column arguments) are available'
              ' for omniscidb 5.4 or newer, '
              'currently connected to omniscidb '
              + '.'.join(map(str, available_version)))
    available_version = ()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = os.path.splitext(os.path.basename(__file__))[0]
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))

    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN', 'DOUBLE']
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b', 'd']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});'
        .format(**locals()))

    data = defaultdict(list)
    for i in range(5):
        for j, n in enumerate(colnames):
            if n == 'b':
                data[n].append(i % 2 == 0)
            elif n == 'd':
                data[n].append(i + 1.5)
            else:
                data[n].append(i)

    m.load_table_columnar(table_name, **data)
    m.table_name = table_name
    yield m
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))


def test_sizer_row_multiplier_orig(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<double>)')
    def my_row_copier_mul(x, m, y):
        input_row_count = len(x)
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i] * 2
        return m * input_row_count

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_mul(cursor(select f8 '
        'from {omnisci.table_name}), 2));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == (float((i % 5) * 2),)


def test_sizer_row_multiplier_param1(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, double, int32, RowMultiplier,'
             'OutputColumn<double>)')
    def my_row_copier_mul_param1(x, alpha, beta, m, y):
        input_row_count = len(x)
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i] * alpha + beta
        return m * input_row_count

    alpha = 3

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_mul_param1('
        'cursor(select f8 from {omnisci.table_name}),'
        'cast({alpha} as double),'
        'cast(4 as int), 2));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == ((i % 5) * alpha + 4,)


def test_sizer_row_multiplier_param2(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(double, Column<double>, int32, RowMultiplier,'
             'OutputColumn<double>)')
    def my_row_copier_mul_param2(alpha, x, beta, m, y):
        input_row_count = len(x)
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i] * alpha + beta
        return m * input_row_count

    alpha = 3

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_mul_param2('
        'cast({alpha} as double),'
        'cursor(select f8 from {omnisci.table_name}),'
        'cast(4 as int), 2));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == ((i % 5) * alpha + 4,)


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with constant parameter"
        " support (got %s) [issue 124]" % (
            available_version,)))
def test_sizer_constant_parameter(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, ConstantParameter, OutputColumn<double>)')
    def my_row_copier_cp(x, m, y):
        n = len(x)
        for i in range(m):
            j = i % n
            y[i] = x[j] * 2
        return m

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_cp(cursor(select f8 '
        'from {omnisci.table_name}), 3));'
        .format(**locals()))
    result = list(result)
    assert len(result) == 3
    for i, r in enumerate(result):
        assert r == (i * 2,)

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_cp(cursor(select f8 '
        'from {omnisci.table_name}), 8));'
        .format(**locals()))
    result = list(result)
    assert len(result) == 8
    for i, r in enumerate(result):
        assert r == ((i % 5) * 2,)


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with constant parameter"
        " support (got %s) [issue 124]" % (
            available_version,)))
def test_sizer_return_size(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, OutputColumn<double>)')
    def my_row_copier_c(x, y):
        for i in range(13):
            j = i % len(x)
            y[i] = x[j] * 2
        return 13

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_c(cursor(select f8 '
        'from {omnisci.table_name})));'
        .format(**locals()))
    result = list(result)
    assert len(result) == 13
    for i, r in enumerate(result):
        assert r == ((i % 5) * 2,), repr((i, r))


@pytest.mark.skipif(
    available_version < (5, 4),
    reason=(
        "test requires omniscidb v 5.4 or newer (got %s) [issue 148]" % (
            available_version,)))
def test_rowmul_add_columns(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, Column<double>, double,'
             ' RowMultiplier, OutputColumn<double>)')
    def add_columns(x, y, alpha, m, r):
        input_row_count = len(x)
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                r[j] = x[i] + alpha * float(y[i])
        return m * input_row_count

    alpha = 2.5

    descr, result = omnisci.sql_execute(
        'select * from table(add_columns('
        'cursor(select f8 from {omnisci.table_name}),'
        ' cursor(select d from {omnisci.table_name}),'
        ' cast({alpha} as double), 1));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == (i + alpha * (i + 1.5),)


@pytest.mark.skipif(
    available_version <= (5, 4),
    reason=(
        "test requires omniscidb with multiple output"
        " columns support (got %s) [issue 150]" % (
            available_version,)))
def test_rowmul_return_mixed_columns(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, RowMultiplier,'
             ' OutputColumn<double>, OutputColumn<float>)')
    def ret_mixed_columns(x, m, y, z):
        input_row_count = len(x)
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = 2 * x[i]
                z[j] = np.float32(3 * x[i])
        return m * input_row_count

    descr, result = omnisci.sql_execute(
        'select * from table(ret_mixed_columns('
        f'cursor(select f8 from {omnisci.table_name}), 1));')

    for i, r in enumerate(result):
        assert r[0] == float(2 * i)
        assert r[1] == float(3 * i)

    descr, result = omnisci.sql_execute(
        'select * from table(ret_mixed_columns('
        f'cursor(select f8 from {omnisci.table_name}), 2));')

    for i, r in enumerate(result):
        assert r[0] == float(2 * (i % 5))
        assert r[1] == float(3 * (i % 5))

    descr, result = omnisci.sql_execute(
        'select out1, out0 from table(ret_mixed_columns('
        f'cursor(select f8 from {omnisci.table_name}), 1));')

    for i, r in enumerate(result):
        assert r[1] == float(2 * i)
        assert r[0] == float(3 * i)


@pytest.mark.skipif(
    available_version < (5, 4),
    reason=(
        "test requires omniscidb with multiple output"
        " columns support (got %s) [issue 150]" % (
            available_version,)))
@pytest.mark.parametrize("max_n", [-1, 3])
@pytest.mark.parametrize("sizer", [1, 2])
@pytest.mark.parametrize("num_columns", [1, 2, 3, 4])
def test_rowmul_return_multi_columns(omnisci, num_columns, sizer, max_n):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    if num_columns == 1:
        @omnisci('int32(int32, Column<double>, RowMultiplier,'
                 ' OutputColumn<double>)')
        def ret_1_columns(n, x1, m, y1):
            input_row_count = len(x1)
            for i in range(input_row_count):
                for c in range(m):
                    j = i + c * input_row_count
                    y1[j] = float((j+1))
            if n < 0:
                return m * input_row_count
            return n

    if num_columns == 2:
        @omnisci('int32(int32, Column<double>, RowMultiplier,'
                 ' OutputColumn<double>, OutputColumn<double>)')
        def ret_2_columns(n, x1, m, y1, y2):
            input_row_count = len(x1)
            for i in range(input_row_count):
                for c in range(m):
                    j = i + c * input_row_count
                    y1[j] = float((j+1))
                    y2[j] = float(2*(j+1))
            if n < 0:
                return m * input_row_count
            return n

    if num_columns == 3:
        @omnisci('int32(int32, Column<double>, RowMultiplier,'
                 ' OutputColumn<double>, OutputColumn<double>,'
                 ' OutputColumn<double>)')
        def ret_3_columns(n, x1, m, y1, y2, y3):
            input_row_count = len(x1)
            for i in range(input_row_count):
                for c in range(m):
                    j = i + c * input_row_count
                    y1[j] = float((j+1))
                    y2[j] = float(2*(j+1))
                    y3[j] = float(3*(j+1))
            if n < 0:
                return m * input_row_count
            return n

    if num_columns == 4:
        @omnisci('int32(int32, Column<double>, RowMultiplier,'
                 ' OutputColumn<double>, OutputColumn<double>,'
                 ' OutputColumn<double>, OutputColumn<double>)')
        def ret_4_columns(n, x1, m, y1, y2, y3, y4):
            input_row_count = len(x1)
            for i in range(input_row_count):
                for c in range(m):
                    j = i + c * input_row_count
                    y1[j] = float((j+1))
                    y2[j] = float(2*(j+1))
                    y3[j] = float(3*(j+1))
                    y4[j] = float(4*(j+1))
            if n < 0:
                return m * input_row_count
            return n

    descr, result = omnisci.sql_execute(
        f'select * from table(ret_{num_columns}_columns({max_n}, '
        f'cursor(select f8 from {omnisci.table_name}), {sizer}));')

    result = list(result)

    if max_n == -1:
        assert len(result) == sizer * 5
    else:
        assert len(result) == max_n
    for i, r in enumerate(result):
        for j, v in enumerate(r):
            assert v == float((i+1) * (j+1))


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with single cursor"
        " support (got %s) [issue 173]" % (
            available_version,)))
@pytest.mark.parametrize("variant", [1, 2, 3])
def test_issue173(omnisci, variant):
    omnisci.reset()
    omnisci.register()

    def mask_zero(x, b, m, y):
        for i in range(len(x)):
            if b[i]:
                y[i] = 0.0
            else:
                y[i] = x[i]
        return len(x)

    descr, result = omnisci.sql_execute(
        f'select f8, b from {omnisci.table_name}')
    f8, b = zip(*list(result))

    # remove after resolving rbc issue 175:
    mask_zero.__name__ += f'_{variant}'

    if variant == 1:
        omnisci('int32(Column<double>, Column<bool>, RowMultiplier,'
                ' OutputColumn<double>)')(mask_zero)
        descr, result = omnisci.sql_execute(
            f'select * from table({mask_zero.__name__}('
            f'cursor(select f8 from {omnisci.table_name}),'
            f'cursor(select b from {omnisci.table_name}), 1))')
    elif variant == 2:
        omnisci('int32(Cursor<Column<double>, Column<bool>>, RowMultiplier,'
                ' OutputColumn<double>)')(mask_zero)

        descr, result = omnisci.sql_execute(
            f'select * from table({mask_zero.__name__}('
            f'cursor(select f8, b from {omnisci.table_name}), 1))')
    elif variant == 3:
        omnisci('int32(Cursor<double, bool>, RowMultiplier,'
                ' OutputColumn<double>)')(mask_zero)

        descr, result = omnisci.sql_execute(
            f'select * from table({mask_zero.__name__}('
            f'cursor(select f8, b from {omnisci.table_name}), 1))')
    else:
        raise ValueError(variant)

    result = list(result)

    for i in range(len(result)):
        assert result[i][0] == (0.0 if b[i] else f8[i])


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with udtf redefine"
        " support (got %s) [issue 175]" % (
            available_version,)))
def test_redefine(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    descr, result = omnisci.sql_execute(
        f'select f8, i4 from {omnisci.table_name}')

    f8, i4 = zip(*result)

    @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<double>)')  # noqa: E501, F811
    def redefined_udtf(x, m, y):  # noqa: E501, F811
        for i in range(len(x)):
            y[i] = x[i] + 1
        return len(x)

    descr, result = omnisci.sql_execute(
        'select * from table(redefined_udtf(cursor('
        f'select f8 from {omnisci.table_name}), 1))')
    result = list(result)
    for i in range(len(result)):
        assert result[i][0] == f8[i] + 1

    # redefine implementation
    @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<double>)')  # noqa: E501, F811
    def redefined_udtf(x, m, y):  # noqa: E501, F811
        for i in range(len(x)):
            y[i] = x[i] + 2
        return len(x)

    descr, result = omnisci.sql_execute(
        'select * from table(redefined_udtf(cursor('
        f'select f8 from {omnisci.table_name}), 1))')
    result = list(result)
    for i in range(len(result)):
        assert result[i][0] == f8[i] + 2

    # overload with another type
    @omnisci('int32(Column<int32>, RowMultiplier, OutputColumn<int32>)')  # noqa: E501, F811
    def redefined_udtf(x, m, y):  # noqa: E501, F811
        for i in range(len(x)):
            y[i] = x[i] + 3
        return len(x)

    descr, result = omnisci.sql_execute(
        'select * from table(redefined_udtf(cursor('
        f'select i4 from {omnisci.table_name}), 1))')
    result = list(result)
    for i in range(len(result)):
        assert result[i][0] == i4[i] + 3

    # check overload
    descr, result = omnisci.sql_execute(
        'select * from table(redefined_udtf(cursor('
        f'select f8 from {omnisci.table_name}), 1))')
    result = list(result)
    for i in range(len(result)):
        assert result[i][0] == f8[i] + 2


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with udtf redefine"
        " support (got %s) [issue 175]" % (
            available_version,)))
@pytest.mark.parametrize("step", [1, 2, 3])
def test_overload_nonuniform(omnisci, step):
    omnisci.reset()
    omnisci.register()

    if step > 0:
        @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<int64>)')  # noqa: E501, F811
        def overloaded_udtf(x, m, y):  # noqa: E501, F811
            y[0] = 64
            return 1

    if step > 1:
        @omnisci('int32(Column<float>, RowMultiplier, OutputColumn<int64>)')  # noqa: E501, F811
        def overloaded_udtf(x, m, y):  # noqa: E501, F811
            y[0] = 32
            return 1

    if step > 2:
        @omnisci('int32(Column<float>, Column<float>, RowMultiplier, OutputColumn<int64>)')  # noqa: E501, F811
        def overloaded_udtf(x1, x2, m, y):  # noqa: E501, F811
            y[0] = 132
            return 1

    sql_query = ('select * from table(overloaded_udtf(cursor('
                 f'select f8 from {omnisci.table_name}), 1))')
    if step > 0:
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)
        assert result[0][0] == 64

    sql_query = ('select * from table(overloaded_udtf(cursor('
                 f'select f4 from {omnisci.table_name}), 1))')
    if step > 1:
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)
        assert result[0][0] == 32
    else:
        with pytest.raises(
                Exception,
                match=(r".*(Function overloaded_udtf\(COLUMN<FLOAT>, INTEGER\) not supported"
                       r"|Could not bind overloaded_udtf\(COLUMN<FLOAT>, INTEGER\))")):
            descr, result = omnisci.sql_execute(sql_query)

    sql_query = ('select * from table(overloaded_udtf(cursor('
                 f'select f4, f4 from {omnisci.table_name}), 1))')
    if step > 2:
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)
        assert result[0][0] == 132
    else:
        with pytest.raises(
                Exception,
                match=(r".*(Function overloaded_udtf\(COLUMN<FLOAT>,"
                       r" COLUMN<FLOAT>, INTEGER\) not supported|Could not bind"
                       r" overloaded_udtf\(COLUMN<FLOAT>, COLUMN<FLOAT>, INTEGER\))")):
            descr, result = omnisci.sql_execute(sql_query)


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with udtf overload"
        " support (got %s) [issue 182]" % (
            available_version,)))
def test_overload_uniform(omnisci):
    omnisci.reset()
    omnisci.register()

    @omnisci('int32(Column<T>, RowMultiplier, OutputColumn<T>)',
             T=['double', 'float', 'int64', 'int32', 'int16', 'int8'])
    def mycopy(x, m, y):  # noqa: E501, F811
        for i in range(len(x)):
            y[i] = x[i]
        return len(x)

    for colname in ['f8', 'f4', 'i8', 'i4', 'i2', 'i1', 'b']:
        sql_query = (f'select {colname} from {omnisci.table_name}')
        descr, result = omnisci.sql_execute(sql_query)
        expected = list(result)
        sql_query = ('select * from table(mycopy(cursor('
                     f'select {colname} from {omnisci.table_name}), 1))')
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)
        assert result == expected


omnisci_aggregators = [
    'avg', 'min', 'max', 'sum', 'count',
    'approx_count_distinct', 'sample', 'single_value', 'stddev',
    'stddev_pop', 'stddev_samp', 'variance', 'var_pop', 'var_samp',
]
omnisci_aggregators2 = ['correlation', 'corr', 'covar_pop', 'covar_samp']
# TODO: round_to_digit, mod, truncate
omnisci_functions = ['sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'abs', 'ceil', 'degrees',
                     'exp', 'floor', 'ln', 'log', 'log10', 'radians', 'round', 'sign', 'sqrt']
omnisci_functions2 = ['atan2', 'power']
omnisci_unary_operations = ['+', '-']
omnisci_binary_operations = ['+', '-', '*', '/']


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with aggregate udtf column"
        " support (got %s) [issue 174]" % (
            available_version,)))
@pytest.mark.parametrize("prop", ['', 'groupby'])
@pytest.mark.parametrize("oper", omnisci_aggregators + omnisci_aggregators2)
def test_column_aggregate(omnisci, prop, oper):
    if not omnisci.has_cuda and oper in omnisci_aggregators2 and prop == 'groupby':
        pytest.skip(f'{oper}-{prop} test crashes CPU-only omnisci server [rbc issue 237]')

    omnisci.reset()
    omnisci.register()

    if oper == 'single_value':
        if prop:
            return

        @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<double>)')
        def test_rbc_last(x, m, y):
            y[0] = x[len(x)-1]
            return 1

        sql_query = (f'select f8 from {omnisci.table_name}')
        descr, result = omnisci.sql_execute(sql_query)
        expected_result = list(result)[-1:]

        sql_query = (f'select {oper}(out0) from table(test_rbc_last(cursor('
                     f'select f8 from {omnisci.table_name}), 1))')
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)

        assert result == expected_result
        return

    @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<double>)')
    def test_rbc_mycopy(x, m, y):
        for i in range(len(x)):
            y[i] = x[i]
        return len(x)

    @omnisci('int32(Cursor<double, double>, RowMultiplier,'
             ' OutputColumn<double>, OutputColumn<double>)')
    def test_rbc_mycopy2(x1, x2, m, y1, y2):
        for i in range(len(x1)):
            y1[i] = x1[i]
            y2[i] = x2[i]
        return len(x1)

    @omnisci('int32(Cursor<double, bool>, RowMultiplier,'
             ' OutputColumn<double>, OutputColumn<bool>)')
    def test_rbc_mycopy2b(x1, x2, m, y1, y2):
        for i in range(len(x1)):
            y1[i] = x1[i]
            y2[i] = x2[i]
        return len(x1)

    @omnisci('int32(Cursor<double, double, bool>, RowMultiplier,'
             'OutputColumn<double>, OutputColumn<double>, OutputColumn<bool>)')
    def test_rbc_mycopy3(x1, x2, x3, m, y1, y2, y3):
        for i in range(len(x1)):
            y1[i] = x1[i]
            y2[i] = x2[i]
            y3[i] = x3[i]
        return len(x1)

    extra_args = ''
    if oper == 'approx_count_distinct':
        extra_args = ', 1'

    mycopy = 'test_rbc_mycopy'

    if oper in omnisci_aggregators2:
        if prop == 'groupby':
            sql_query_expected = (
                f'select {oper}(f8, d) from {omnisci.table_name} group by b')
            sql_query = (
                f'select {oper}(out0, out1) from table({mycopy}3(cursor('
                f'select f8, d, b from {omnisci.table_name}), 1)) group by out2')
        else:
            sql_query_expected = (f'select {oper}(f8, d) from {omnisci.table_name}')
            sql_query = (
                f'select {oper}(out0, out1) from table({mycopy}2(cursor('
                f'select f8, d from {omnisci.table_name}), 1))')
    else:
        if prop == 'groupby':
            sql_query_expected = (
                f'select {oper}(f8{extra_args}) from {omnisci.table_name} group by b')
            sql_query = (
                f'select {oper}(out0) from table({mycopy}2b(cursor('
                f'select f8, b from {omnisci.table_name}), 1)) group by out1')
        else:
            sql_query_expected = (f'select {oper}(f8{extra_args}) from {omnisci.table_name}')
            sql_query = (
                f'select {oper}(out0) from table({mycopy}(cursor('
                f'select f8 from {omnisci.table_name}), 1))')

    descr, result_expected = omnisci.sql_execute(sql_query_expected)
    result_expected = list(result_expected)

    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    assert result == result_expected


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with aggregate udtf column"
        " support (got %s) [issue 174]" % (
            available_version,)))
@pytest.mark.parametrize("oper", omnisci_functions + omnisci_functions2)
def test_column_function(omnisci, oper):
    omnisci.reset()
    omnisci.register()

    @omnisci('int32(Column<double>, RowMultiplier, OutputColumn<double>)')
    def test_rbc_mycopy(x, m, y):
        for i in range(len(x)):
            y[i] = x[i]
        return len(x)

    @omnisci('int32(Cursor<double, double>, RowMultiplier,'
             ' OutputColumn<double>, OutputColumn<double>)')
    def test_rbc_mycopy2(x1, x2, m, y1, y2):
        for i in range(len(x1)):
            y1[i] = x1[i]
            y2[i] = x2[i]
        return len(x1)

    mycopy = 'test_rbc_mycopy'

    if oper in omnisci_functions2:
        sql_query_expected = (f'select {oper}(f8, d) from {omnisci.table_name}')
        sql_query = (
            f'select {oper}(out0, out1) from table({mycopy}2(cursor('
            f'select f8, d from {omnisci.table_name}), 1))')
    else:
        sql_query_expected = (f'select {oper}(f8) from {omnisci.table_name}')
        sql_query = (
            f'select {oper}(out0) from table({mycopy}(cursor('
            f'select f8 from {omnisci.table_name}), 1))')

    descr, result_expected = omnisci.sql_execute(sql_query_expected)
    result_expected = list(result_expected)

    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    if oper in ['asin', 'acos']:
        # result contains nan
        assert repr(result) == repr(result_expected)
    else:
        assert result == result_expected


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with aggregate udtf column"
        " support (got %s) [issue 174]" % (
            available_version,)))
@pytest.mark.parametrize("oper", omnisci_binary_operations)
def test_column_binary_operation(omnisci, oper):
    omnisci.reset()
    omnisci.register()

    @omnisci('int32(Cursor<double, double>, RowMultiplier,'
             ' OutputColumn<double>, OutputColumn<double>)')
    def test_rbc_mycopy2(x1, x2, m, y1, y2):
        for i in range(len(x1)):
            y1[i] = x1[i]
            y2[i] = x2[i]
        return len(x1)

    mycopy = 'test_rbc_mycopy'

    sql_query_expected = (f'select f8 {oper} d from {omnisci.table_name}')
    sql_query = (
        f'select out0 {oper} out1 from table({mycopy}2(cursor('
        f'select f8, d from {omnisci.table_name}), 1))')

    descr, result_expected = omnisci.sql_execute(sql_query_expected)
    result_expected = list(result_expected)

    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    assert result == result_expected


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb with aggregate udtf column"
        " support (got %s) [issue 174]" % (
            available_version,)))
@pytest.mark.parametrize("oper", omnisci_unary_operations)
def test_column_unary_operation(omnisci, oper):
    omnisci.reset()
    omnisci.register()

    @omnisci('int32(Cursor<double>, RowMultiplier,'
             ' OutputColumn<double>)')
    def test_rbc_mycopy(x1, m, y1):
        for i in range(len(x1)):
            y1[i] = x1[i]
        return len(x1)

    mycopy = 'test_rbc_mycopy'

    sql_query_expected = (f'select {oper} f8 from {omnisci.table_name}')
    sql_query = (
        f'select {oper} out0 from table({mycopy}(cursor('
        f'select f8 from {omnisci.table_name}), 1))')

    descr, result_expected = omnisci.sql_execute(sql_query_expected)
    result_expected = list(result_expected)

    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    assert result == result_expected


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test requires omniscidb create as support"
        " (got %s)" % (
            available_version,)))
def test_create_as(omnisci):
    omnisci.reset()
    omnisci.register()

    @omnisci('int32(Cursor<double>, RowMultiplier,'
             ' OutputColumn<double>)')
    def test_rbc_mycopy(x1, m, y1):
        for i in range(len(x1)):
            y1[i] = x1[i]
        return len(x1)

    mycopy = 'test_rbc_mycopy'

    sql_query_expected = (f'select f8 from {omnisci.table_name}')
    sql_queries = [
        f'DROP TABLE IF EXISTS {omnisci.table_name}_f8',
        (f'CREATE TABLE {omnisci.table_name}_f8 AS SELECT out0 FROM TABLE({mycopy}(CURSOR('
         f'SELECT f8 FROM {omnisci.table_name}), 1))'),
        f'SELECT * FROM {omnisci.table_name}_f8']

    descr, result_expected = omnisci.sql_execute(sql_query_expected)
    result_expected = list(result_expected)

    for sql_query in sql_queries:
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)

    assert result == result_expected


@pytest.fixture(scope='function')
def create_columns(omnisci):
    # delete tables
    omnisci.sql_execute('DROP TABLE IF EXISTS datatable;')
    omnisci.sql_execute('DROP TABLE IF EXISTS kerneltable;')
    # create tables
    omnisci.sql_execute('CREATE TABLE IF NOT EXISTS datatable (x DOUBLE);')
    omnisci.sql_execute('CREATE TABLE IF NOT EXISTS kerneltable (kernel DOUBLE);')
    # add data
    omnisci.load_table_columnar('datatable', **{'x': [1.0, 2.0, 3.0, 4.0, 5.0]})
    omnisci.load_table_columnar('kerneltable', **{'kernel': [10.0, 20.0, 30.0]})
    yield omnisci
    # delete tables
    omnisci.sql_execute('DROP TABLE IF EXISTS datatable;')
    omnisci.sql_execute('DROP TABLE IF EXISTS kerneltable;')


@pytest.mark.skipif(
    available_version < (5, 5),
    reason=(
        "test with different column sizes requires omnisci 5.5"
        " support (got %s) [issue 176]" % (
            available_version,)))
@pytest.mark.usefixtures('create_columns')
def test_column_different_sizes(omnisci):

    @omnisci('int32(Column<double>, Column<double>, RowMultiplier, OutputColumn<double>)')
    def convolve(x, kernel, m, y):
        for i in range(len(y)):
            y[i] = 0.0
        for i in range(len(x)):
            for j in range(len(kernel)):
                k = i + j
                if (k < len(x)):
                    y[k] += kernel[j] * x[k]
        # output has the same size as @x
        return len(x)

    _, result = omnisci.sql_execute(
        'select * from table('
        'convolve(cursor(select x from datatable),'
        'cursor(select kernel from kerneltable), 1))')

    expected = [(10.0,), (60.0,), (180.0,), (240.0,), (300.0,)]
    assert list(result) == expected
