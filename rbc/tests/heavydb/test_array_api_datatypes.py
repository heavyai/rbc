import numpy as np
import pytest

from rbc.stdlib import array_api, datatypes
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), load_test_data=False):
        yield o


@pytest.mark.parametrize('dtype', datatypes.__all__)
def test_datatypes(heavydb, dtype):
    heavydb.unregister()

    @heavydb(f'{dtype}[](int32)')
    def test_datatype(size):
        return array_api.ones(size, dtype=dtype)

    if dtype.startswith('uint') or dtype.startswith('complex'):
        with pytest.raises(ValueError, match=".*cannot convert.*"):
            test_datatype(5).execute()
    else:
        expected = np.ones(5, dtype=dtype)
        got = test_datatype(5).execute()
        np.testing.assert_equal(expected, got)
