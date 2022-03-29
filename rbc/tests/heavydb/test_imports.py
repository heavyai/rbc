import pytest


classes = [
    'Array',
    'ArrayPointer',
    'Bytes',
    'Column',
]


@pytest.mark.parametrize('cls', classes)
def test_heavydb_imports(cls):
    try:
        exec(f'from rbc.heavydb import {cls}')
    except ImportError:
        pytest.fail(f'Could not import "rbc.heavydb.{cls}')
