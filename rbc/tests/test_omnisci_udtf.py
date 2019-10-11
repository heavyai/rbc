import pytest
rbc_omnisci = pytest.importorskip('rbc.mapd')


def omnisci_is_available():
    """Return True if OmniSci server is accessible.
    """
    omnisci = rbc_omnisci.RemoteMapD()
    client = omnisci.make_client()
    try:
        version = client(MapD=dict(get_version=()))['MapD']['get_version']
    except Exception as msg:
        return False, 'failed to get OmniSci version: %s' % (msg)
    if version >= '4.6':
        return True, None
    return False, 'expected OmniSci version 4.6 or greater, got %s' % (version)


is_available, reason = omnisci_is_available()
pytestmark = pytest.mark.skipif(not is_available, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    m = rbc_omnisci.RemoteMapD(debug=True,
                               use_host_target=True)
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

    # @omnisci('|table(int32*, double*, int64, int64*, double*|output)')
    def my_row_copier1(error_code, x, input_row_count, output_row_count, y):
        # sizer type is Constant
        m = 5
        n = m * input_row_count
        for i in range(input_row_count):
            for c in range(m):
                y[i + c * input_row_count] = x[i]
        output_row_count[0] = n
        error_code[0] = 0

    # @omnisci('|table(int32*, double*, int64, int64, int64*, double*|output)')
    def my_row_copier2(error_code, x,
                       n: dict(sizer='kUserSpecifiedConstantParameter'),
                       input_row_count, output_row_count, y):
        m = 5
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                if j < n:
                    y[j] = x[i]
                else:
                    break
        output_row_count[0] = n
        error_code[0] = 0

    @omnisci('|table(int32*, double*, int64, int64, int64*, double*|output)')
    def my_row_copier3(error_code, x,
                       m: dict(sizer='kUserSpecifiedRowMultiplier'),
                       input_row_count, output_row_count, y):
        for i in range(input_row_count):
            break
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i]
        output_row_count[0] = m * input_row_count
        error_code[0] = 0
    #omnisci.reset()
    omnisci.register()
    #return
    # select d, count(*) from table(row_copier(cursor(SELECT d
    #                     FROM tf_test GROUP BY d), 5)) GROUP BY d ORDER BY d;
    descr, result = omnisci.sql_execute(
        'select f8, * from table(my_row_copier3(cursor(select f8 '
        'from {omnisci.table_name}), 2));'
        .format(**locals()))
    print(descr)
    for r in result:
        print(r)
