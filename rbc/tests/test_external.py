import pytest
from rbc.tests import omnisci_fixture
from rbc.external import external
from numba import types
import numpy as np
import math


@pytest.fixture(scope="module")
def omnisci():

    for o in omnisci_fixture(globals()):
        define(o)
        yield o


cmath = (
    # Trigonometric
    ("cos", "float64(float64)"),
    ("sin", "float64(float64)"),
    ("tan", "float64(float64)"),
    ("acos", "float64(float64)"),
    ("asin", "float64(float64)"),
    ("atan", "float64(float64)"),
    ("atan2", "float64(float64, float64)"),
    # Hyperbolic
    ("cosh", "float64(float64)"),
    ("sinh", "float64(float64)"),
    ("tanh", "float64(float64)"),
    ("acosh", "float64(float64)"),
    ("asinh", "float64(float64)"),
    ("atanh", "float64(float64)"),
    # Exponential and logarithmic functions
    ("exp", "float64(float64)"),
    ("frexp", "float64(float64)"),
    ("ldexp", "float64(float64, int64)"),
    ("log", "float64(float64)"),
    ("log10", "float64(float64)"),
    ("modf", "float64(float64)"),
    ("exp2", "float64(float64)"),
    ("expm1", "float64(float64)"),
    ("ilogb", "float64(float64)"),
    ("log1p", "float64(float64)"),
    ("log2", "float64(float64)"),
    ("logb", "float64(float64)"),
    # power functions
    ("pow", "float64(float64, float64)"),
    ("sqrt", "float64(float64)"),
    ("cbrt", "float64(float64)"),
    ("hypot", "float64(float64, float64)"),
    # error and gamma functions
    ("erf", "float64(float64)"),
    ("erfc", "float64(float64)"),
    ("tgamma", "float64(float64)"),
    ("lgamma", "float64(float64)"),
    # Rounding
    ("ceil", "float64(float64)"),
    ("floor", "float64(float64)"),
    ("fmod", "float64(float64, float64)"),
    ("trunc", "float64(float64)"),
    ("round", "float64(float64)"),
    ("lround", "int64(float64)"),
    ("llround", "int64(float64)"),
    ("rint", "float64(float64)"),
    ("lrint", "int64(float64)"),
    ("llrint", "int64(float64)"),
    ("nearbyint", "float64(float64)"),
    ("remainder", "float64(float64, float64)"),
    # Floating-point manipulation
    ("copysign", "float64(float64, float64)"),
    ("nan", "float64(float64)"),
    ("nextafter", "float64(float64, float64)"),
    ("nexttoward", "float64(float64, float64)"),
    # Minimum, maximum, difference functions
    ("fdim", "float64(float64, float64)"),
    ("fmax", "float64(float64, float64)"),
    ("fmin", "float64(float64, float64)"),
    # Other functions
    ("fabs", "float64(float64)"),
    ("abs", "int64(int64)"),
    ("fma", "float64(float64, float64, float64)"),
)


def define(omnisci):
    def inner(fname, signature):
        cmath_fn = external(signature, name=fname)
        retty = str(cmath_fn.return_type)
        argtypes = tuple(map(str, cmath_fn.args))
        arity = len(cmath_fn.signature.args)

        # define omnisci callable
        if arity == 1:

            def fn(a):
                return cmath_fn(a)

        elif arity == 2:

            def fn(a, b):
                return cmath_fn(a, b)

        else:

            def fn(a, b, c):
                return cmath_fn(a, b, c)

        fn.__name__ = f"{omnisci.table_name}_{fname}"
        fn = omnisci(f"{retty}({', '.join(argtypes)})")(fn)

    for _fname, signature in cmath:
        inner(_fname, signature)


@pytest.mark.parametrize("fname,sig", cmath)
def test_external_cmath(omnisci, fname, sig):

    if fname in ["logb", "ilogb"]:
        pytest.skip(f"cmath function {fname} not supported")

    if fname in ["frexp", "modf", "nan"]:
        pytest.skip(f"cmath function {fname} crashes omniscidb server")

    if fname in ["remainder"]:
        pytest.skip(f"cmath.{fname} wrong output!")

    table = omnisci.table_name
    cmath_func = f"{table}_{fname}"

    remap = {
        "acos": "arccos",
        "asin": "arcsin",
        "atan": "arctan",
        "atan2": "arctan2",
        "acosh": "arccosh",
        "asinh": "arcsinh",
        "atanh": "arctanh",
        "pow": "power",
        "tgamma": "gamma",
        "lround": "round",
        "llround": "round",
        "lrint": "round",
        "llrint": "round",
        "nearbyint": "round",
        "nexttoward": "nextafter",
        "fdim": "subtract",
    }

    mod = math if fname in ["erf", "erfc", "tgamma", "lgamma"] else np
    fn = getattr(mod, remap.get(fname, fname)) if fname != "fma" else None

    if fname in ["acos", "asin", "atan"]:
        query = f"SELECT f8/10.0, {cmath_func}(f8/10.0) from {table}"
    elif fname in ["atan2"]:
        query = f"SELECT f8/10.0, f8/8.0, {cmath_func}(f8/10.0, f8/8.0) FROM {table}"
    elif fname in [
        "pow",
        "hypot",
        "fmod",
        "remainder",
        "nextafter",
        "nexttoward",
        "fdim",
        "fmax",
        "fmin",
    ]:
        query = f"SELECT f8+10.0, f8+1.0, {cmath_func}(f8+10.0, f8+1.0) FROM {table}"
    elif fname == "copysign":
        query = f"SELECT f8, -1*f8, {cmath_func}(f8, -1*f8) FROM {table}"
    elif fname == "fma":
        query = f"SELECT f8, f8, f8, {cmath_func}(f8, f8, f8) FROM {table}"
    elif fname == "ldexp":
        query = f"SELECT f8+1.0, 2, {cmath_func}(f8+1.0, 2) FROM {table}"
    elif fname == "atanh":
        query = f"SELECT f8/8.0, {cmath_func}(f8/8.0) from {table}"
    elif fname == "abs":
        query = f"SELECT -1*i8, {cmath_func}(-1*i8) from {table}"
    else:
        query = f"SELECT f8+10.0, {cmath_func}(f8+10.0) from {table}"

    _, result = omnisci.sql_execute(query)

    for values in result:
        if fname in [
            "atan2",
            "pow",
            "ldexp",
            "hypot",
            "fmod",
            "remainder",
            "copysign",
            "nextafter",
            "nexttoward",
            "fdim",
            "fmax",
            "fmin",
        ]:
            a, b, r = values
            assert np.isclose(r, fn(a, b))
        elif fname == "fma":
            a, b, c, r = values
            assert np.isclose(r, a * b + c)
        else:
            a, r = values
            assert np.isclose(r, fn(a))


def test_valid_signatures(omnisci):
    assert external("f64 log2(f64)").name == "log2"
    assert external("f64(f64)", name="log2").name == "log2"


def test_invalid_signature(omnisci):
    with pytest.raises(ValueError) as excinfo:
        external(types.int64, name="test")

    assert "signature must represent a function type" in str(excinfo)


def test_unnamed_external(omnisci):
    with pytest.raises(ValueError) as excinfo:
        external("f64(f64)")

    assert "external function name not specified for signature" in str(excinfo)


def test_replace_declaration(omnisci):

    _ = external("f64(f64)", name="fma")
    fma = external("f64(f64, f64, f64)", name="fma")

    @omnisci("double(double, double, double)")
    def test_fma(a, b, c):
        return fma(a, b, c)

    _, result = omnisci.sql_execute('select test_fma(1.0, 2.0, 3.0)')

    assert list(result) == [(5.0,)]
