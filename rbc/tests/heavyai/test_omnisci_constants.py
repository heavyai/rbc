import pytest
import numpy as np
from rbc.tests import omnisci_fixture
from rbc.stdlib import array_api
from collections import OrderedDict


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), load_test_data=False):
        define(o)
        yield o


def define(omnisci):
    @omnisci('float64(int32)')
    def get_constant(typ):
        if typ == 0:
            return array_api.e
        if typ == 1:
            return array_api.inf
        if typ == 2:
            return array_api.nan
        return array_api.pi


constants_map = OrderedDict(e=np.e, inf=np.inf, nan=np.nan, pi=np.pi)


@pytest.mark.parametrize('C', constants_map)
def test_constants(omnisci, C):
    idx = list(constants_map.keys()).index(C)
    _, result = omnisci.sql_execute(f'select get_constant({idx});')

    expected = constants_map[C]
    if np.isnan(expected):
        assert np.isnan(list(result)[0])
    else:
        assert list(result) == [(expected,)]
