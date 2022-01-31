import numpy as np
import pytest
from rbc.externals.omniscidb import set_output_row_size
from rbc.tests import omnisci_fixture, assert_equal


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals()):
        define(o)
        yield o


def define(omnisci):

    @omnisci('T(T)', T=['int32', 'int64', 'float32', 'float64'])
    def myincr(x):
        return x + 1

    T = ['int64', 'float64', 'int32']
    @omnisci('UDTF(int32 | name=size, T, OutputColumn<T> | name=x)', T=T, devices=['cpu'])
    def arange(size, x0, x):
        set_output_row_size(size)
        for i in range(size):
            x[i] = x0 + x.dtype(i)
        return size

    T = ['int64', 'float32']
    @omnisci('UDTF(Column<T> | name=x, T, OutputColumn<T> | name=y)', T=T, devices=['cpu'])
    def aincr(x, dx, y):
        size = len(x)
        set_output_row_size(size)
        for i in range(size):
            y[i] = x[i] + dx
        return size


def test_remote_udf_evaluation(omnisci):
    myincr = omnisci.get_caller('myincr')

    assert myincr(3) == 4
    assert myincr(3.5) == 4.5

    assert_equal(myincr(np.int64(3)), np.int64(4))
    # The following test fails because SELECT seems to upcast int32 to
    # int64. TODO: investigate.
    # assert_equal(myincr(np.int32(3)), np.int32(4))

    assert_equal(myincr(np.float32(3.5)), np.float32(4.5))
    # The following test fails because SELECT seems to downcast
    # float64 to float32. TODO: investigate.
    # assert_equal(myincr(np.float64(3.5)), np.float64(4.5))


def test_remote_udtf_evaluation(omnisci):
    arange = omnisci.get_caller('arange')

    assert_equal(str(arange(3, 1)),
                 'Query(SELECT x FROM TABLE(arange(CAST(3 AS INT), CAST(1 AS BIGINT))))')

    assert_equal(arange(3, 1)['x'], list(np.arange(3, dtype=np.int64) + 1))
    assert_equal(arange(3, 1.5)['x'], list(np.arange(3, dtype=np.float64) + 1.5))
    assert_equal(arange(np.int32(3), 1)['x'], list(np.arange(3, dtype=np.int32) + 1))

    assert_equal(arange(3, np.float32(1))['x'], np.arange(3, dtype=np.float32) + 1)
    # The following test fails because SELECT seems to downcast
    # float64 to float32. TODO: investigate.
    # assert_equal(arange(3, np.float64(1))['x'], np.arange(3, dtype=np.float64) + 1)

    assert_equal(arange(3, np.int64(1))['x'], np.arange(3, dtype=np.int64) + 1)
    # The following test fails because SELECT seems to upcast int32 to
    # int64. TODO: investigate.
    # assert_equal(arange(3, np.int32(1))['x'], np.arange(3, dtype=np.int32) + 1)


def test_remote_composite_udtf_evaluation(omnisci):
    arange = omnisci.get_caller('arange')
    aincr = omnisci.get_caller('aincr')

    r = aincr(arange(3, 1), 2)

    assert_equal(str(r), 'Query(SELECT y FROM TABLE(aincr(CURSOR(SELECT x FROM'
                 ' TABLE(arange(CAST(3 AS INT), CAST(1 AS BIGINT)))), CAST(2 AS BIGINT))))')

    r = r.execute()

    assert_equal(r['y'], np.arange(3, dtype=np.int64) + 1 + 2)
