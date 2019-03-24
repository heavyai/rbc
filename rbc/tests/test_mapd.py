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


def test_simple():
    mapd = rbc_mapd.RemoteMapD()

    @mapd('f64(f64, f64)', 'i64(i64,i64)')
    def add(x, y):
        return x + y

    print(add.get_MapD_version())
    descr, r = add.sql_execute(
        'select dest_merc_x, dest_merc_y from flights_2008_10k limit 10')
    add.register()
