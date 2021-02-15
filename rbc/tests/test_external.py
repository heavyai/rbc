import pytest
from rbc.tests import omnisci_fixture
from rbc.external import external
from rbc.typesystem import Type
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
    ("cos", "float64 cos(float64)"),
    ("sin", "float64 sin(float64)"),
    ("tan", "float64 tan(float64)"),
    ("acos", "float64 acos(float64)"),
    ("asin", "float64 asin(float64)"),
    ("atan", "float64 atan(float64)"),
    ("atan2", "float64 atan2(float64, float64)"),
    # Hyperbolic
    ("cosh", "float64 cosh(float64)"),
    ("sinh", "float64 sinh(float64)"),
    ("tanh", "float64 tanh(float64)"),
    ("acosh", "float64 acosh(float64)"),
    ("asinh", "float64 asinh(float64)"),
    ("atanh", "float64 atanh(float64)"),
    # Exponential and logarithmic functions
    ("exp", "float64 exp(float64)"),
    ("frexp", "float64 frexp(float64)"),
    ("ldexp", "float64 ldexp(float64, int64)"),
    ("log", "float64 log(float64)"),
    ("log10", "float64 log10(float64)"),
    ("modf", "float64 modf(float64)"),
    ("exp2", "float64 exp2(float64)"),
    ("expm1", "float64 expm1(float64)"),
    ("ilogb", "float64 ilogb(float64)"),
    ("log1p", "float64 log1p(float64)"),
    ("log2", "float64 log2(float64)"),
    ("logb", "float64 logb(float64)"),
    # power functions
    ("pow", "float64 pow(float64, float64)"),
    ("sqrt", "float64 sqrt(float64)"),
    ("cbrt", "float64 cbrt(float64)"),
    ("hypot", "float64 hypot(float64, float64)"),
    # error and gamma functions
    ("erf", "float64 erf(float64)"),
    ("erfc", "float64 erfc(float64)"),
    ("tgamma", "float64 tgamma(float64)"),
    ("lgamma", "float64 lgamma(float64)"),
    # Rounding
    ("ceil", "float64 ceil(float64)"),
    ("floor", "float64 floor(float64)"),
    ("fmod", "float64 fmod(float64, float64)"),
    ("trunc", "float64 trunc(float64)"),
    ("round", "float64 round(float64)"),
    ("lround", "int64 lround(float64)"),
    ("llround", "int64 llround(float64)"),
    ("rint", "float64 rint(float64)"),
    ("lrint", "int64 lrint(float64)"),
    ("llrint", "int64 llrint(float64)"),
    ("nearbyint", "float64 nearbyint(float64)"),
    ("remainder", "float64 remainder(float64, float64)"),
    # Floating-point manipulation
    ("copysign", "float64 copysign(float64, float64)"),
    ("nan", "float64 nan(float64)"),
    ("nextafter", "float64 nextafter(float64, float64)"),
    ("nexttoward", "float64 nexttoward(float64, float64)"),
    # Minimum, maximum, difference functions
    ("fdim", "float64 fdim(float64, float64)"),
    ("fmax", "float64 fmax(float64, float64)"),
    ("fmin", "float64 fmin(float64, float64)"),
    # Other functions
    ("fabs", "float64 fabs(float64)"),
    ("abs", "int64 abs(int64)"),
    ("fma", "float64 fma(float64, float64, float64)"),
)


def define(omnisci):
    def inner(fname, signature):
        cmath_fn = external(signature)
        t = Type.fromstring(signature)
        retty = str(t[0])
        argtypes = tuple(map(str, t[1]))
        arity = len(argtypes)

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

    _ = external("f64 fma(f64)")
    fma = external("f64 fma(f64, f64, f64)")

    @omnisci("double(double, double, double)")
    def test_fma(a, b, c):
        return fma(a, b, c)

    _, result = omnisci.sql_execute("select test_fma(1.0, 2.0, 3.0)")

    assert list(result) == [(5.0,)]


def test_require_target_info(omnisci):

    log2 = external("double log2(double)")

    @omnisci("double(double)")
    def test_log2(a):
        return log2(a)

    _, result = omnisci.sql_execute("select test_log2(8.0)")

    assert list(result) == [(3.0,)]
