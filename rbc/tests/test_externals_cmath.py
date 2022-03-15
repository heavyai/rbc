import sys
import atexit
import pytest
from rbc.tests import heavydb_fixture
from rbc.remotejit import RemoteJIT
from rbc.externals import cmath
from rbc.typesystem import Type
import numpy as np
import math


@pytest.fixture(scope="module")
def rjit(request):
    local = False
    rjit = RemoteJIT(debug=not True, local=local)
    if not local:
        rjit.start_server(background=True)
        request.addfinalizer(rjit.stop_server)
        atexit.register(rjit.stop_server)
    define(rjit)
    return rjit


@pytest.fixture(scope="module")
def ljit(request):
    ljit = RemoteJIT(debug=not True, local=True)
    define(ljit)
    return ljit


@pytest.fixture(scope="module")
def heavydb():

    for o in heavydb_fixture(globals()):
        define(o)
        yield o


cmath_funcs = (
    # Trigonometric
    ("cos", "float64 cos(float64)|CPU"),
    ("sin", "float64 sin(float64)|CPU"),
    ("tan", "float64 tan(float64)|CPU"),
    ("acos", "float64 acos(float64)|CPU"),
    ("asin", "float64 asin(float64)|CPU"),
    ("atan", "float64 atan(float64)|CPU"),
    ("atan2", "float64 atan2(float64, float64)|CPU"),
    # Hyperbolic
    ("cosh", "float64 cosh(float64)|CPU"),
    ("sinh", "float64 sinh(float64)|CPU"),
    ("tanh", "float64 tanh(float64)|CPU"),
    ("acosh", "float64 acosh(float64)|CPU"),
    ("asinh", "float64 asinh(float64)|CPU"),
    ("atanh", "float64 atanh(float64)|CPU"),
    # Exponential and logarithmic functions
    ("exp", "float64 exp(float64)|CPU"),
    ("frexp", "float64 frexp(float64)|CPU"),
    ("ldexp", "float64 ldexp(float64, int64)|CPU"),
    ("log", "float64 log(float64)|CPU"),
    ("log10", "float64 log10(float64)|CPU"),
    ("modf", "float64 modf(float64, float64*)|CPU"),
    ("exp2", "float64 exp2(float64)|CPU"),
    ("expm1", "float64 expm1(float64)|CPU"),
    ("ilogb", "float64 ilogb(float64)|CPU"),
    ("log1p", "float64 log1p(float64)|CPU"),
    ("log2", "float64 log2(float64)|CPU"),
    ("logb", "float64 logb(float64)|CPU"),
    # power functions
    ("pow", "float64 pow(float64, float64)|CPU"),
    ("sqrt", "float64 sqrt(float64)|CPU"),
    ("cbrt", "float64 cbrt(float64)|CPU"),
    ("hypot", "float64 hypot(float64, float64)|CPU"),
    # error and gamma functions
    ("erf", "float64 erf(float64)|CPU"),
    ("erfc", "float64 erfc(float64)|CPU"),
    ("tgamma", "float64 tgamma(float64)|CPU"),
    ("lgamma", "float64 lgamma(float64)|CPU"),
    # Rounding
    ("ceil", "float64 ceil(float64)|CPU"),
    ("floor", "float64 floor(float64)|CPU"),
    ("fmod", "float64 fmod(float64, float64)|CPU"),
    ("trunc", "float64 trunc(float64)|CPU"),
    ("round", "float64 round(float64)|CPU"),
    ("lround", "int64 lround(float64)|CPU"),
    ("llround", "int64 llround(float64)|CPU"),
    ("rint", "float64 rint(float64)|CPU"),
    ("lrint", "int64 lrint(float64)|CPU"),
    ("llrint", "int64 llrint(float64)|CPU"),
    ("nearbyint", "float64 nearbyint(float64)|CPU"),
    ("remainder", "float64 remainder(float64, float64)|CPU"),
    # Floating-point manipulation
    ("copysign", "float64 copysign(float64, float64)|CPU"),
    ("nan", "float64 nan(const char*)|CPU"),
    ("nextafter", "float64 nextafter(float64, float64)|CPU"),
    ("nexttoward", "float64 nexttoward(float64, float64)|CPU"),
    # Minimum, maximum, difference functions
    ("fdim", "float64 fdim(float64, float64)|CPU"),
    ("fmax", "float64 fmax(float64, float64)|CPU"),
    ("fmin", "float64 fmin(float64, float64)|CPU"),
    # Other functions
    ("fabs", "float64 fabs(float64)|CPU"),
    ("abs", "int64 abs(int64)|CPU"),
    ("fma", "float64 fma(float64, float64, float64)|CPU"),
)


funcs = {}


def _get_query_func(fname):
    PREFIX = "test_cmath"
    return f"{PREFIX}_{fname}"


def define(jit):
    global funcs

    def inner(cmath_func, query_func, signature):
        cmath_fn = getattr(cmath, cmath_func)
        t = Type.fromstring(signature)
        retty = str(t[0])
        argtypes = tuple(map(str, t[1]))
        arity = len(argtypes)

        # define callable
        if arity == 1:
            def fn(a):
                return cmath_fn(a)

        elif arity == 2:
            def fn(a, b):
                return cmath_fn(a, b)

        else:
            def fn(a, b, c):
                return cmath_fn(a, b, c)

        fn.__name__ = query_func
        fn = jit(f"{retty}({', '.join(argtypes)})", devices=["cpu"])(fn)
        return fn

    blocklist = ("frexp", "nan")

    for fname, signature in cmath_funcs:
        if fname not in blocklist:
            query_func = _get_query_func(fname)
            fn = inner(fname, query_func, signature)
            funcs[(jit, query_func)] = fn


def _get_pyfunc(fname):
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

    if fname == "fma":
        return lambda a, b, c: a*b + c
    if fname == "ilogb":
        return lambda a: np.trunc(np.log2(a))
    if fname == "logb":
        return np.log2

    mod = math if fname in ["erf", "erfc", "tgamma", "lgamma"] else np
    fn = getattr(mod, remap.get(fname, fname))
    return fn


@pytest.mark.parametrize("fname,sig", cmath_funcs, ids=[item[0] for item in cmath_funcs])
def test_external_cmath_heavydb(heavydb, fname, sig):

    if fname in ["logb", "ilogb", "modf"]:
        pytest.skip(f"cmath function {fname} not supported")

    if fname in ["frexp", "nan"]:
        pytest.xfail(f"cmath function {fname} crashes heavydb server")

    if fname in ["remainder"]:
        pytest.xfail(f"cmath.{fname} wrong output!")

    table = heavydb.table_name
    query_func = _get_query_func(fname)
    pyfunc = _get_pyfunc(fname)

    if fname in ["acos", "asin", "atan"]:
        query = f"SELECT f8/10.0, {query_func}(f8/10.0) from {table}"
    elif fname in ["atan2"]:
        query = f"SELECT f8/10.0, f8/8.0, {query_func}(f8/10.0, f8/8.0) FROM {table}"
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
        query = f"SELECT f8+10.0, f8+1.0, {query_func}(f8+10.0, f8+1.0) FROM {table}"
    elif fname == "copysign":
        query = f"SELECT f8, -1*f8, {query_func}(f8, -1*f8) FROM {table}"
    elif fname == "fma":
        query = f"SELECT f8, f8, f8, {query_func}(f8, f8, f8) FROM {table}"
    elif fname == "ldexp":
        query = f"SELECT f8+1.0, 2, {query_func}(f8+1.0, 2) FROM {table}"
    elif fname == "atanh":
        query = f"SELECT f8/8.0, {query_func}(f8/8.0) from {table}"
    elif fname == "abs":
        query = f"SELECT -1*i8, {query_func}(-1*i8) from {table}"
    else:
        query = f"SELECT f8+10.0, {query_func}(f8+10.0) from {table}"

    _, result = heavydb.sql_execute(query)

    for values in result:
        if len(values) == 3:
            a, b, r = values
            assert np.isclose(r, pyfunc(a, b)), fname
        elif len(values) == 4:
            a, b, c, r = values
            assert np.isclose(r, pyfunc(a, b, c)), fname
        else:
            a, r = values
            assert np.isclose(r, pyfunc(a)), fname


@pytest.fixture(scope="module")
def input_data():
    return {
        'f8': np.arange(5, dtype=np.float64),
        'i8': np.arange(5, dtype=np.int64)
    }


@pytest.mark.parametrize("location", ['local', 'remote'])
@pytest.mark.parametrize("fname,sig", cmath_funcs, ids=[item[0] for item in cmath_funcs])
def test_external_cmath_remotejit(input_data, location, ljit, rjit, fname, sig):

    if fname in ["modf", "frexp", "nan"]:
        pytest.skip(f"cmath function {fname} requires a pointer argument")

    if fname in ["remainder", "ilogb", "logb"]:
        pytest.xfail(f"cmath function {fname} returns the wrong value")

    if fname in ["nexttoward"] and sys.platform == "darwin":
        pytest.xfail(f"{fname} fails on {sys.platform}")

    jit = rjit if location == 'remote' else ljit
    query_func = _get_query_func(fname)
    fn = funcs.get((jit, query_func))

    i8 = input_data['i8']
    f8 = input_data['f8']

    pyfunc = _get_pyfunc(fname)

    if fname in ["acos", "asin", "atan"]:
        args = (f8/10.0,)
    elif fname in ["logb"]:
        args = ((f8+1)*10.0,)
    elif fname in ["atan2"]:
        args = (f8/10.0, f8/8.0)
    elif fname in ["pow", "hypot", "fmod", "remainder", "nextafter",
                   "nexttoward", "fdim", "fmax", "fmin"]:
        args = (f8+10.0, f8+1.0)
    elif fname == "copysign":
        args = (f8, -1*f8)
    elif fname == "fma":
        args = (f8, f8, f8)
    elif fname == "ldexp":
        args = f8+1.0, np.full_like(f8, 2, dtype=np.int32)
    elif fname == "atanh":
        args = (f8/8.0,)
    elif fname == "abs":
        args = (-1*i8,)
    else:
        args = (f8 + 10.0,)

    result = list(map(lambda inputs: fn(*inputs), zip(*args)))

    for values in zip(*args, result):
        if len(values) == 3:
            a, b, r = values
            assert np.isclose(r, pyfunc(a, b)), fname
        elif len(values) == 4:
            a, b, c, r = values
            assert np.isclose(r, a * b + c), fname
        else:
            a, r = values
            assert np.isclose(r, pyfunc(a)), fname
