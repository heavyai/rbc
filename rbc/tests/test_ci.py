import os
import sys
import pytest
from rbc.tests import heavydb_fixture
from rbc.utils import parse_version


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), load_test_data=False):
        yield o


def test_python_version():
    varname = 'EXPECTED_PYTHON_VERSION'
    current = tuple(sys.version_info)
    expected = os.environ.get(varname)
    if os.environ.get('CI'):
        assert expected is not None, (varname, current)
    if expected is None:
        pytest.skip(
            f'Undefined environment variable {varname},'
            f' cannot test python version (current={".".join(map(str, current))})')
    expected = parse_version(expected)
    current_stripped = current[:len(expected)]
    assert expected == current_stripped


def test_heavydb_version(heavydb):
    varname = 'EXPECTED_HEAVYDB_VERSION'
    current = heavydb.version
    expected = os.environ.get(varname)
    if 'CI' in os.environ and expected is None:
        pytest.fail("HeavyDB server is not running")
    if expected is None:
        pytest.skip(
            f'Undefined environment variable {varname},'
            f' cannot test heavydb version (current={".".join(map(str, current))})')
    if expected == 'dev':
        assert current[:2] >= (5, 8), current  # TODO: update dev version periodically
        pytest.skip(
            f'heavydb dev version is {".".join(map(str, current))}')
    expected = parse_version(expected)
    current_stripped = current[:len(expected)]
    assert expected == current_stripped


def test_numba_version():
    varname = 'EXPECTED_NUMBA_VERSION'
    import numba
    current = parse_version(numba.__version__)
    expected = os.environ.get(varname)
    if expected is None:
        pytest.skip(
            f'Undefined environment variable {varname},'
            f' cannot test numba version (current={".".join(map(str, current))})')
    expected = parse_version(expected)
    current_stripped = current[:len(expected)]
    assert expected == current_stripped
