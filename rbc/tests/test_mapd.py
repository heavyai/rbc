import pytest
rbc_mapd = pytest.importorskip('rbc.mapd')


def mapd_is_available():
    """Return True if MapD server is accessible.
    """
    mapd = rbc_mapd.RemoteMapD()
    client = mapd.make_client()
    try:
        version = client(MapD=dict(get_version=()))['MapD']['get_version']
    except Exception as msg:
        return False, 'failed to get MapD version: %s' % (msg)
    if version >= '4.6':
        return True, None
    return False, 'expected MapD version 4.6 or greater, got %s' % (version)


is_available, reason = mapd_is_available()
pytestmark = pytest.mark.skipif(not is_available, reason=reason)


@pytest.fixture(scope='module')
def mapd():
    m = rbc_mapd.RemoteMapD()
    table_name = 'rbc_test_mapd'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN']
    # todo: TEXT ENCODING DICT, TEXT ENCODING NONE, TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://www.omnisci.com/docs/latest/5_datatypes.html
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


def test_redefine(mapd):
    mapd.reset()

    @mapd('f64(f64)')
    def incr(x):
        return x + 1

    desrc, result = mapd.sql_execute(
        'select i4, incr(i4) from {mapd.table_name}'.format(**locals()))
    for x, x1 in result:
        assert x1 == x + 1

    @mapd('f64(f64)')  # noqa: F811
    def incr(x):
        return x + 2

    desrc, result = mapd.sql_execute(
        'select i4, incr(i4) from {mapd.table_name}'.format(**locals()))
    for x, x1 in result:
        assert x1 == x + 2


def test_single_argument_overloading(mapd):
    mapd.reset()

    @mapd(
        'f64(f64)',
        'i64(i64)',
        'i32(i32)',
        'f32(f32)',
        'i32(f32)',
    )
    def mydecr(x):
        return x - 1
    desrc, result = mapd.sql_execute(
        'select f4, mydecr(f4) from {mapd.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))
    desrc, result = mapd.sql_execute(
        'select f8, mydecr(f8) from {mapd.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))
    desrc, result = mapd.sql_execute(
        'select i4, mydecr(i4) from {mapd.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))


def test_thrift_api_doc(mapd):
    mapd.reset()

    @mapd('double(int, double)',
          'float(int, float)',
          'int(int, int)')
    def foo(i, v):
        return v * i + 55

    descr, result = mapd.sql_execute(
        'select f8, foo(i4, f8) from {mapd.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for i, (x, x1) in enumerate(result):
        assert x1 == x * i + 55
        assert isinstance(x1, type(x))


def test_multiple_implementation(mapd):
    mapd.reset()

    @mapd('int(f64)', 'int(i64)')  # noqa: F811
    def bits(x):
        return 64

    @mapd('int(f32)', 'int(i32)')  # noqa: F811
    def bits(x):
        return 32

    @mapd('int(i16)')  # noqa: F811
    def bits(x):
        return 16

    @mapd('int(i8)')  # noqa: F811
    def bits(x):
        return 8

    descr, result = mapd.sql_execute(
        'select bits(i1), bits(i2), bits(i4), bits(f4), bits(i8), bits(f8)'
        ' from {mapd.table_name} limit 1'.format(**locals()))
    result = list(result)
    assert len(result) == 1
    assert result[0] == (8, 16, 32, 32, 64, 64)
