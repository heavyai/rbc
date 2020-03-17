import pytest


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


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
