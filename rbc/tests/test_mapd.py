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


def create_test_table(mapd, datatype='DOUBLE'):
    table_name = 'test_table_{datatype}'.format(datatype=datatype)
    drop_test_table(mapd, table_name)
    mapd.sql_execute('''\
CREATE TABLE IF NOT EXISTS test_table_{datatype} (
    i INT,
    x {datatype},
    y {datatype}
    );'''.format(datatype=datatype))
    data = [
        (1, 1.0, 0.0),
        (2, 2.0, 0.0),
        (3, 2.0, 1.0),
        (4, 1.0, 2.0),
        (5, 1.0, 1.0),
    ]
    for i, x, y in data:
        mapd.sql_execute(
            'INSERT INTO test_table_{datatype} VALUES ({i}, {x}, {y})'
            .format(datatype=datatype, i=i, x=x, y=y))
    descr, result = mapd.sql_execute('SELECT * FROM test_table_{datatype}'.format(datatype=datatype))
    assert data == list(result)
    return table_name

def drop_test_table(mapd, table_name):
    mapd.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(table_name=table_name))


def test_redefine():
    mapd = rbc_mapd.RemoteMapD()
    table_name = create_test_table(mapd)

    @mapd('f64(f64)')
    def incr(x):
        return x + 1

    desrc, result = mapd.sql_execute('select x, incr_dadA(x) from {table_name}'.format(table_name=table_name))

    for x, x1 in result:
        assert x1 == x + 1

    @mapd('f64(f64)')
    def incr(x):
        return x + 2

    desrc, result = mapd.sql_execute('select x, incr_dadA(x) from {table_name}'.format(table_name=table_name))

    for x, x1 in result:
        assert x1 == x + 2

    drop_test_table(mapd, table_name)


def test_simple():
    mapd = rbc_mapd.RemoteMapD()

    @mapd('f64(f64, f64)', 'i64(i64,i64)')
    def add(x, y):
        return x + 2*y

    #print(add.get_MapD_version())
    #add.register()
    if 1:
        descr, r = add.sql_execute(
            'select add_daddA(dest_merc_x, dest_merc_y) from flights_2008_10k limit 10')
        print(descr)
        print(r)
        print('\n'.join(str(s) for s in r))

