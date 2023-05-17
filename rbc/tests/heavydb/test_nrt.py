import pytest

from rbc.tests import heavydb_fixture, override_config


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), load_test_data=False):
        yield o


def test_nrt_warning(heavydb):

    @heavydb("i32(i32)", devices=['cpu'])
    def list_append(t):
        lst = list()
        lst.append('abc')
        return len(lst)

    with override_config('ENABLE_NRT', 0):
        with pytest.warns(UserWarning, match='NRT required but not enabled'):
            heavydb.register()
