import os
import sys
import pytest
from rbc.tests import omnisci_fixture
from rbc.utils import parse_version


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals()):
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


def test_omniscidb_version(omnisci):
    varname = 'EXPECTED_OMNISCIDB_VERSION'
    current = omnisci.version
    expected = os.environ.get(varname)
    if 'CI' in os.environ and expected is None:
        pytest.fail("OmniSciDB server is not running")
    if expected is None:
        pytest.skip(
            f'Undefined environment variable {varname},'
            f' cannot test omniscidb version (current={".".join(map(str, current))})')
    if expected == 'dev':
        assert current[:2] >= (5, 8), current  # TODO: update dev version periodically
        pytest.skip(
            f'omniscidb dev version is {".".join(map(str, current))}')
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
