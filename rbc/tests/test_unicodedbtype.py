import pytest
from rbc.irtools import read_unicodetype_db


@pytest.fixture(scope='module')
def db():
    return read_unicodetype_db()


@pytest.mark.usefixtures('db')
def test_unicodedbtype_functions(db):
    # ensure there's a unicodetype_db.ll file and it contains the
    # required symbols
    required_fns = ('numba_PyUnicode_ToNumeric', 'numba_PyUnicode_IsWhitespace',
                    'numba_PyUnicode_IsLinebreak', 'numba_gettyperecord',
                    'numba_get_PyUnicode_ExtendedCase')
    fns = list(db.functions)
    assert len(fns) == len(required_fns)
    for fn in db.functions:
        assert fn.name in required_fns


@pytest.mark.usefixtures('db')
def test_unicodedbtype_globals(db):

    required_globals = ('numba_PyUnicode_TypeRecords', 'index1', 'index2',
                        'numba_PyUnicode_ExtendedCase')
    for glob in db.global_variables:
        assert glob.name in required_globals


@pytest.mark.usefixtures('db')
def test_numba_PyUnicode_TypeRecord_struct(db):

    try:
        db.get_struct_type('struct.numba_PyUnicode_TypeRecord')
    except NameError:
        pytest.fail('Raise exception')
