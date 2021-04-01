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
    if expected is None:
        pytest.skip(
            f'Undefined environment variable {varname},'
            f' cannot test omniscidb version (current={".".join(map(str, current))})')
    expected = parse_version(expected)
    current_stripped = current[:len(expected)]
    assert expected == current_stripped


def test_numba_version():
    varname = 'EXPECTED_NUMBA_VERSION'
    import numba
    current = numba.version_info.full
    expected = os.environ.get(varname)
    if expected is None:
        pytest.skip(
            f'Undefined environment variable {varname},'
            f' cannot test numba version (current={".".join(map(str, current))})')
    expected = parse_version(expected)
    current_stripped = current[:len(expected)]
    assert expected == current_stripped
