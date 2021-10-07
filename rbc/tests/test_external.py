import pytest
from rbc.tests import omnisci_fixture
from rbc.external import external
from numba import types


@pytest.fixture(scope="module")
def omnisci():
    for o in omnisci_fixture(globals()):
        yield o


def test_valid_signatures(omnisci):
    external("f64 log2(f64)")
    assert True


def test_invalid_signature(omnisci):
    with pytest.raises(ValueError) as excinfo:
        external(types.int64)

    assert "signature must represent a function type" in str(excinfo)


def test_unnamed_external(omnisci):
    with pytest.raises(ValueError) as excinfo:
        external("f64(f64)")

    assert "external function name not specified for signature" in str(excinfo)

    with pytest.raises(ValueError) as excinfo:
        external("f64(f64)")

    assert "external function name not specified for signature" in str(excinfo)


def test_replace_declaration(omnisci):

    _ = external("f64 fma(f64)|CPU")
    fma = external("f64 fma(f64, f64, f64)|CPU")

    @omnisci("double(double, double, double)", devices=["cpu"])
    def test_fma(a, b, c):
        return fma(a, b, c)

    _, result = omnisci.sql_execute("select test_fma(1.0, 2.0, 3.0)")

    assert list(result) == [(5.0,)]


def test_require_target_info(omnisci):

    log2 = external("double log2(double)|CPU")

    @omnisci("double(double)", devices=["cpu"])
    def test_log2(a):
        return log2(a)

    _, result = omnisci.sql_execute("select test_log2(8.0)")

    assert list(result) == [(3.0,)]
