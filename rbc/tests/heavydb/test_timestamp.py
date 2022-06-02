from rbc.heavydb import Timestamp
from rbc.tests import heavydb_fixture
import pytest

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), debug=not True, load_columnar=True, load_test_data=False):
        yield o


def test_timestamp(heavydb):

    @heavydb("Timestamp(int64)")
    def toto(t):
        return Timestamp(t)

    heavydb.register()
    print(toto.describe())
    #toto(4).execute()

