import pytest
import importlib


classes = [
    'Array',
    'ArrayPointer',
    'TextEncodingNone',
    'Column',
]


@pytest.mark.parametrize('cls', classes)
def test_heavydb_imports(cls):
    try:
        importlib.import_module('rbc.heavydb', cls)
    except ImportError:
        pytest.fail(f'Could not import "rbc.heavydb.{cls}')
