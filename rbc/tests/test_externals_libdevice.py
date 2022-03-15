import pytest
from functools import reduce
from typing import Tuple
from rbc.tests import heavydb_fixture
from rbc.externals import libdevice

libdevicefuncs = pytest.importorskip("numba.cuda.libdevicefuncs")

funcs = []
for fname, (retty, args) in libdevicefuncs.functions.items():
    assert fname.startswith('__nv_'), fname
    argtys = tuple(map(lambda x: str(x.ty), args))
    arity = len(argtys)
    has_ptr_arg = reduce(lambda x, y: x or y.is_ptr, args, False)
    funcs.append((fname, str(retty), argtys, has_ptr_arg))


@pytest.fixture(scope="module")
def heavydb():

    for o in heavydb_fixture(globals()):
        if not o.has_cuda:
            pytest.skip("cuda is not enabled")
        define(o)
        yield o


def define(heavydb):
    def inner(fname: str, retty: str, argtypes: Tuple[str]):
        cmath_fn = getattr(libdevice, fname)
        arity = len(argtypes)

        # define heavydb callable
        if arity == 1:

            def fn(a):
                return cmath_fn(a)

        elif arity == 2:

            def fn(a, b):
                return cmath_fn(a, b)

        else:

            def fn(a, b, c):
                return cmath_fn(a, b, c)

        fn.__name__ = f"{heavydb.table_name}_{fname[5:]}"
        fn = heavydb(f"{retty}({', '.join(argtypes)})", devices=["gpu"])(fn)

    for fname, retty, argtys, has_ptr_arg in funcs:
        if has_ptr_arg:
            continue
        inner(fname, str(retty), argtys)


cols_dict = {
    "float32": "f4",
    "float64": "f8",
    "int16": "i2",
    "int32": "i4",
    "int64": "i8",
}


@pytest.mark.slow
@pytest.mark.parametrize(
    "fname,retty,argtys,has_ptr_arg", funcs, ids=[item[0] for item in funcs]
)
def test_externals_libdevice(heavydb, fname, retty, argtys, has_ptr_arg):
    if has_ptr_arg:
        pytest.skip(f"{fname} has a pointer argument")

    func_name = f"{heavydb.table_name}_{fname[5:]}"
    table = f"{heavydb.table_name}"

    if fname[5:] in ["brev", "brevll"]:
        query = f"SELECT {func_name}(i4+4) FROM {table}"
    elif fname[5:] in ["ilogb", "ilogbf"]:
        query = f"SELECT {func_name}({cols_dict[argtys[0]]}+2.0) FROM {table}"
    else:
        cols = ", ".join(tuple(map(lambda x: cols_dict[x], argtys)))
        query = f"SELECT {func_name}({cols}) FROM {table}"

    _, _ = heavydb.sql_execute(query)
