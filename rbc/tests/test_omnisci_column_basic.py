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
    True,
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

    @omnisci('int32(Column<double>, ConstantParameter, Column<double>)')
    def my_row_copier_cp(x, m, y):
        input_row_count = len(x)
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i] * 2
        return m

    descr, result = omnisci.sql_execute(
        'select * from table(my_row_copier_cp(cursor(select f8 '
        'from {omnisci.table_name}), 5));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == ((i % 5) * 2,)


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
