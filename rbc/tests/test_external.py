import pytest
from rbc.tests import omnisci_fixture
from rbc.external import external
import numpy as np
import math


@pytest.fixture(scope="module")
def omnisci():

    for o in omnisci_fixture(globals()):
        define(o)
        yield o


cmath = (
    # Trigonometric
    "cos",
    "sin",
    "tan",
    "acos",
    "asin",
    "atan",
    "atan2",
    # Hyperbolic
    "cosh",
    "sinh",
    "tanh",
    "acosh",
    "asinh",
    "atanh",
    # Exponential and logarithmic functions
    "exp",
    "frexp",
    "ldexp",
    "log",
    "log10",
    "modf",
    "exp2",
    "expm1",
    "ilogb",
    "log1p",
    "log2",
    "logb",
    # power functions
    "pow",
    "sqrt",
    "cbrt",
    "hypot",
    # error and gamma functions
    "erf",
    "erfc",
    "tgamma",
    "lgamma",
    # Rounding
    "ceil",
    "floor",
    "fmod",
    "trunc",
    "round",
    "lround",
    "llround",
    "rint",
    "lrint",
    "llrint",
    "nearbyint",
    "remainder",
    # Floating-point manipulation
    "copysign",
    "nan",
    "nextafter",
    "nexttoward",
    # Minimum, maximum, difference functions
    "fdim",
    "fmax",
    "fmin",
    # Other functions
    "fabs",
    "abs",
    "fma",
)


def define(omnisci):
    def inner(fname, retty="float64", argtypes=["float64"]):
        cmath_fn = external(f"{retty} {fname}({', '.join(argtypes)})")

        # define omnisci callable
        if len(argtypes) == 1:

            def fn(a):
                return cmath_fn(a)

        elif len(argtypes) == 2:

            def fn(a, b):
                return cmath_fn(a, b)

        else:

            def fn(a, b, c):
                return cmath_fn(a, b, c)

        fn.__name__ = f"omnisci_{fname}"
        fn = omnisci(f"{retty}({', '.join(argtypes)})")(fn)

    for _fname in cmath:
        if _fname in [
            "atan2",
            "pow",
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
            inner(_fname, argtypes=["float64", "float64"])
        elif _fname == "ldexp":
            inner(_fname, argtypes=["float64", "int64"])
        elif _fname == "abs":
            inner(_fname, retty="int64", argtypes=["int64"])
        elif _fname in ["lround", "llround", "lrint", "llrint"]:
            inner(_fname, retty="int64")
        elif _fname == "fma":
            inner(_fname, argtypes=["float64", "float64", "float64"])
        else:
            inner(_fname)


@pytest.mark.parametrize("fname", cmath)
def test_external_cmath(omnisci, fname):

    if fname in ["logb", "ilogb"]:
        pytest.skip(f"cmath function {fname} not supported")

    if fname in ["frexp", "modf", "nan"]:
        pytest.skip(f"cmath function {fname} crashes omniscidb server")

    if fname in ["remainder"]:
        pytest.skip(f"cmath.{fname} wrong output!")

    table = omnisci.table_name
    cmath_func = "omnisci_" + fname

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
