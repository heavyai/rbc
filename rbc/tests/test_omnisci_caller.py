import numpy as np
import pytest
from rbc.externals.omniscidb import set_output_row_size
from rbc.tests import omnisci_fixture, assert_equal


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals(), minimal_version=(5, 8)):
        define(o)
        yield o


def define(omnisci):

    @omnisci('T(T)', T=['int32', 'int64', 'float32', 'float64'])
    def myincr(x):
        return x + 1

    T = ['int64', 'float64', 'int32']

    @omnisci('UDTF(int32 size, T x0, OutputColumn<T> x)', T=T, devices=['cpu'])
    def arange(size, x0, x):
        set_output_row_size(size)
        for i in range(size):
            x[i] = x0 + x.dtype(i)
        return size

    T = ['int64', 'float32']

    @omnisci('UDTF(Column<T> x, T dx, OutputColumn<T> y)', T=T, devices=['cpu'])
    def aincr(x, dx, y):
        size = len(x)
        set_output_row_size(size)
        for i in range(size):
            y[i] = x[i] + dx
        return size


def test_udf_string_repr(omnisci):
    myincr = omnisci.get_caller('myincr')

    assert_equal(repr(myincr),
                 "RemoteDispatcher('myincr', ['T(T), T=int32|int64|float32|float64'])")
    assert_equal(str(myincr), "myincr['T(T), T=int32|int64|float32|float64']")

    assert_equal(repr(myincr(5)),
                 "OmnisciQueryCapsule('myincr(CAST(5 AS BIGINT))')")
    assert_equal(str(myincr(5)), "myincr(CAST(5 AS BIGINT))")

    assert_equal(repr(myincr(myincr(5))),
                 "OmnisciQueryCapsule('myincr(CAST(myincr(CAST(5 AS BIGINT)) AS BIGINT))')")
    assert_equal(str(myincr(myincr(5))), "myincr(CAST(myincr(CAST(5 AS BIGINT)) AS BIGINT))")


def test_udtf_string_repr(omnisci):
    arange = omnisci.get_caller('arange')

    assert_equal(repr(arange),
                 ("RemoteDispatcher('arange', ['UDTF(int32 size, T x0, OutputColumn<T> x),"
                  " T=int64|float64|int32, device=cpu'])"))
    assert_equal(str(arange),
                 ("arange['UDTF(int32 size, T x0, OutputColumn<T> x),"
                  " T=int64|float64|int32, device=cpu']"))

    assert_equal(repr(arange(5, 0)),
                 ("OmnisciQueryCapsule('SELECT x FROM"
                  " TABLE(arange(CAST(5 AS INT), CAST(0 AS BIGINT)))')"))
    assert_equal(str(arange(5, 0)),
                 "SELECT x FROM TABLE(arange(CAST(5 AS INT), CAST(0 AS BIGINT)))")


def test_remote_udf_evaluation(omnisci):
    myincr = omnisci.get_caller('myincr')

    assert_equal(str(myincr(3)), 'myincr(CAST(3 AS BIGINT))')
    assert_equal(myincr(3, hold=False), 4)
    assert_equal(myincr(3).execute(), 4)

    assert_equal(myincr(3.5).execute(), 4.5)
    assert_equal(myincr(np.int64(3)).execute(), np.int64(4))
    assert_equal(myincr(np.float32(3.5)).execute(), np.float32(4.5))


def test_remote_int32_evaluation(omnisci):
    myincr = omnisci.get_caller('myincr')
    arange = omnisci.get_caller('arange')

    pytest.xfail('SELECT upcasts int32 to int64')
    assert_equal(myincr(np.int32(3)), np.int32(4))
    assert_equal(arange(3, np.int32(1))['x'], np.arange(3, dtype=np.int32) + 1)


def test_remote_float64_evaluation(omnisci):
    myincr = omnisci.get_caller('myincr')
    arange = omnisci.get_caller('arange')

    pytest.xfail('SELECT downcasts float64 to float32')
    assert_equal(myincr(np.float64(3.5)), np.float64(4.5))
    assert_equal(arange(3, np.float64(1))['x'], np.arange(3, dtype=np.float64) + 1)


def test_remote_composite_udf_evaluation(omnisci):
    myincr = omnisci.get_caller('myincr')

    assert_equal(str(myincr(myincr(3))),
                 'myincr(CAST(myincr(CAST(3 AS BIGINT)) AS BIGINT))')
    assert_equal(str(myincr(myincr(3, hold=False))), 'myincr(CAST(4 AS BIGINT))')
    assert_equal(myincr(myincr(3), hold=False), 5)
    assert_equal(myincr(myincr(3)).execute(), 5)


def test_remote_udtf_evaluation(omnisci):
    arange = omnisci.get_caller('arange')

    assert_equal(str(arange(3, 1)),
                 'SELECT x FROM TABLE(arange(CAST(3 AS INT), CAST(1 AS BIGINT)))')

    assert_equal(arange(3, 1)['x'], list(np.arange(3, dtype=np.int64) + 1))
    assert_equal(arange(3, 1.5)['x'], list(np.arange(3, dtype=np.float64) + 1.5))
    assert_equal(arange(np.int32(3), 1)['x'], list(np.arange(3, dtype=np.int32) + 1))
    assert_equal(arange(3, np.float32(1))['x'], np.arange(3, dtype=np.float32) + 1)
    assert_equal(arange(3, np.int64(1))['x'], np.arange(3, dtype=np.int64) + 1)


def test_remote_composite_udtf_evaluation(omnisci):
    arange = omnisci.get_caller('arange')
    aincr = omnisci.get_caller('aincr')
    myincr = omnisci.get_caller('myincr')

    r = aincr(arange(3, 1), 2)
    assert_equal(str(r), 'SELECT y FROM TABLE(aincr(CURSOR(SELECT x FROM'
                 ' TABLE(arange(CAST(3 AS INT), CAST(1 AS BIGINT)))), CAST(2 AS BIGINT)))')

    r = r.execute()
    assert_equal(r['y'], np.arange(3, dtype=np.int64) + 1 + 2)

    r = arange(3, myincr(2, hold=False))
    assert_equal(str(r), 'SELECT x FROM TABLE(arange(CAST(3 AS INT), CAST(3 AS BIGINT)))')
    assert_equal(r['x'], np.arange(3, dtype=np.int64) + 2 + 1)


def test_remote_composite_udtf_udf(omnisci):
    """
    TableFunctionExecutionContext.cpp:277 Check failed:
    col_buf_ptrs.size() == exe_unit.input_exprs.size() (1 == 2)
    """
    myincr = omnisci.get_caller('myincr')
    arange = omnisci.get_caller('arange')

    r = arange(3, myincr(2))
    assert_equal(str(r), ('SELECT x FROM TABLE(arange(CAST(3 AS INT),'
                          ' CAST(myincr(CAST(2 AS BIGINT)) AS BIGINT)))'))

    pytest.xfail('udtf(udf) crashes omniscidb server')
    assert_equal(r['x'], np.arange(3, dtype=np.int64) + 2 + 1)


def test_remote_udf_typeerror(omnisci):
    myincr = omnisci.get_caller('myincr')
    try:
        myincr("abc")
    except TypeError as msg:
        assert_equal(str(msg), '''\
found no matching function signature to given argument types:
    (string) -> ...
  available function signatures:
    (int32) -> int32
    (int64) -> int64
    (float32) -> float32
    (float64) -> float64''')
    else:
        assert 0  # expected TypeError


def test_remote_udtf_typeerror(omnisci):
    arange = omnisci.get_caller('arange')
    try:
        arange(1.2, 0)
    except TypeError as msg:
        assert_equal(str(msg), '''\
found no matching function signature to given argument types:
    (float64, int64) -> ...
  available function signatures:
    (int32 size, int64 x0) -> (Column<int64> x)
    - UDTF(int32 size, int64 x0, OutputColumn<int64> x)
    (int32 size, float64 x0) -> (Column<float64> x)
    - UDTF(int32 size, float64 x0, OutputColumn<float64> x)
    (int32 size, int32 x0) -> (Column<int32> x)
    - UDTF(int32 size, int32 x0, OutputColumn<int32> x)''')
    else:
        assert 0  # expected TypeError


def test_remote_udf_overload(omnisci):

    @omnisci('int32(int32)')  # noqa: F811
    def incr_ol(x):           # noqa: F811
        return x + 1

    @omnisci('int32(int32, int32)')  # noqa: F811
    def incr_ol(x, dx):              # noqa: F811
        return x + dx

    assert incr_ol(1).execute() == 2
    assert incr_ol(1, 2).execute() == 3
