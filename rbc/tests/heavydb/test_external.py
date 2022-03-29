import pytest
from rbc.tests import heavydb_fixture
from rbc.external import external
from numba import types


@pytest.fixture(scope="module")
def heavydb():
    for o in heavydb_fixture(globals()):
        yield o


def test_valid_signatures(heavydb):
    external("f64 log2(f64)")
    assert True


def test_invalid_signature(heavydb):
    with pytest.raises(ValueError) as excinfo:
        external(types.int64)

    assert "signature must represent a function type" in str(excinfo)


def test_unnamed_external(heavydb):
    with pytest.raises(ValueError) as excinfo:
        external("f64(f64)")

    assert "external function name not specified for signature" in str(excinfo)

    with pytest.raises(ValueError) as excinfo:
        external("f64(f64)")

    assert "external function name not specified for signature" in str(excinfo)


def test_replace_declaration(heavydb):

    _ = external("f64 fma(f64)|CPU")
    fma = external("f64 fma(f64, f64, f64)|CPU")

    @heavydb("double(double, double, double)", devices=["cpu"])
    def test_fma(a, b, c):
        return fma(a, b, c)

    _, result = heavydb.sql_execute("select test_fma(1.0, 2.0, 3.0)")

    assert list(result) == [(5.0,)]


def test_require_target_info(heavydb):

    log2 = external("double log2(double)|CPU")

    @heavydb("double(double)", devices=["cpu"])
    def test_log2(a):
        return log2(a)

    _, result = heavydb.sql_execute("select test_log2(8.0)")

    assert list(result) == [(3.0,)]
