import pytest
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), debug=not True, load_columnar=True, load_test_data=False):
        yield o


def test_TextEncodingNone_return(heavydb):
    heavydb.reset()

    with pytest.warns(SyntaxWarning):
        @heavydb('TextEncodingNone(int32, int32)')
        def make_abc(first, n):
            r = TextEncodingNone(n)  # noqa: F821
            for i in range(n):
                r[i] = first + i
            return r
    heavydb.reset()
