import pytest
import math
rbc_omnisci = pytest.importorskip('rbc.omniscidb')


def omnisci_is_available():
    """Return True if OmniSci server is accessible.
    """
    config = rbc_omnisci.get_client_config()
    omnisci = rbc_omnisci.RemoteOmnisci(**config)
    client = omnisci.client
    try:
        version = client(
                Omnisci=dict(get_version=()))['Omnisci']['get_version']
    except Exception as msg:
        return False, 'failed to get OmniSci version: %s' % (msg)
    if version >= '4.6':
        return True, None
    return False, 'expected OmniSci version 4.6 or greater, got %s' % (version)


is_available, reason = omnisci_is_available()
pytestmark = pytest.mark.skipif(not is_available, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = 'rbc_test_omnisci_udtf'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))

    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN']
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});'
        .format(**locals()))

    def row_value(row, col, colname):
        if colname == 'b':
            return ("'true'" if row % 2 == 0 else "'false'")
        return row

    rows = 5
    for i in range(rows):
        table_row = ', '.join(str(row_value(i, j, n))
                              for j, n in enumerate(colnames))
        m.sql_execute(
            'INSERT INTO {table_name} VALUES ({table_row})'.format(**locals()))
    m.table_name = table_name
    yield m
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))


def test_simple(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    def my_row_copier1(x, input_row_count_ptr, output_row_count, y):
        # sizer type is Constant
        m = 5
        input_row_count = input_row_count_ptr[0]
        n = m * input_row_count
        for i in range(input_row_count):
            for c in range(m):
                y[i + c * input_row_count] = x[i]
        output_row_count[0] = n
        return 0

    if 0:
        omnisci('int32|table(double*|cursor, int64*, int64*, double*|output)')(
            my_row_copier1)
        # Exception: Failed to allocate 5612303629517800 bytes of memory
        descr, result = omnisci.sql_execute(
            'select * from table(my_row_copier1(cursor(select f8 '
            'from {omnisci.table_name})));'
            .format(**locals()))

        for i, r in enumerate(result):
            print(i, r)

    def my_row_copier2(x,
                       n_ptr: dict(sizer='kUserSpecifiedConstantParameter'),
                       input_row_count_ptr, output_row_count, y):
        n = n_ptr[0]
        m = 5
        input_row_count = input_row_count_ptr[0]
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                if j < n:
                    y[j] = x[i]
                else:
                    break
        output_row_count[0] = n
        return 0

    if 0:
        omnisci('int32|table(double*|cursor, int32*|input, int64*, int64*,'
                ' double*|output)')(my_row_copier2)
        # Exception: Failed to allocate 5612303562962920 bytes of memory
        descr, result = omnisci.sql_execute(
            'select * from table(my_row_copier2(cursor(select f8 '
            'from {omnisci.table_name}), 2));'
            .format(**locals()))

        for i, r in enumerate(result):
            print(i, r)

    @omnisci('double(double)')
    def myincr(x):
        return x + 1.0

    @omnisci('int32|table(double*|cursor, int32*|input, int64*, int64*,'
             ' double*|output)')
    def my_row_copier3(x,
                       m_ptr: dict(sizer='kUserSpecifiedRowMultiplier'),
                       input_row_count_ptr, output_row_count, y):
        m = m_ptr[0]
        input_row_count = input_row_count_ptr[0]
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i] * 2
        output_row_count[0] = m * input_row_count
        return 0

    descr, result = omnisci.sql_execute(
        'select f8, myincr(f8) from table(my_row_copier3(cursor(select f8 '
        'from {omnisci.table_name}), 2));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == ((i % 5) * 2, (i % 5) * 2 + 1)


def test_math_functions(omnisci):
    omnisci.reset()

    omnisci.sql_execute('drop table if exists {omnisci.table_name}'.format(**locals()))
    omnisci.sql_execute('create table if not exists {omnisci.table_name} (x DOUBLE, i INT)'.format(**locals()))

    for _i in range(5):
        x = float(_i) * 2
        omnisci.sql_execute('insert into {omnisci.table_name} values ({x}, {_i})'.format(**locals()))

    @omnisci('double(double)')  # noqa: F811
    def sin(x):
        return math.sin(x)

    @omnisci('double(double)')  # noqa: F811
    def cos(x):
        return math.cos(x)

    @omnisci('double(double)')  # noqa: F811
    def atan(x):
        return math.atan(x)

    # @omnisci('double(double)')  # noqa: F811
    # def asinh(x):
    #     return np.arcsinh(x) # math.asinh will not work here!

    descr, result = omnisci.sql_execute(
        'select x, sin(x), cos(x), atan(x) from {omnisci.table_name}'
        .format(**locals())
    )

    print(descr, result)
    print(list(result))
    