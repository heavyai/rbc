from rbc.tests import heavydb_fixture
from rbc.heavydb import TextEncodingNone
import pytest

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['text']):
        yield o


def test_list_simple(heavydb):

    @heavydb("i32(TextEncodingNone)", devices=['cpu'])
    def test_list(t):
        l = list()
        l.extend('abc')
        return len(l)

    heavydb.register()

    table = heavydb.table_name + 'text'
    _, result = heavydb.sql_execute(f"select n, test_list(n) from {table} limit 1;")
    print(list(result))


def test_set_simple(heavydb):
    @heavydb("i32(TextEncodingNone)", devices=['cpu'])
    def fn(t):
        s = set('ab')
        return len(s)

    heavydb.register()

    table = heavydb.table_name + 'text'
    _, result = heavydb.sql_execute(f"select n, fn(n) from {table} limit 1;")
    print(list(result))
