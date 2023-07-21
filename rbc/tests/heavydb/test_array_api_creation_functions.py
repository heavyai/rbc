import pytest
import numpy as np
from rbc.tests import heavydb_fixture
from rbc.stdlib import array_api


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), load_test_data=False):
        yield o


def test_asarray(heavydb):
    heavydb.unregister()

    @heavydb('int64[](int64)')
    def asarray(sz):
        return array_api.asarray(list(range(sz)))

    arr = np.arange(5, dtype=np.int64)
    np.testing.assert_array_equal(asarray(5).execute(), arr)


def test_asarray_dtype(heavydb):
    heavydb.unregister()

    @heavydb('float[](int64)')
    def asarray_dtype(sz):
        arr = array_api.arange(sz, dtype=array_api.int64)
        return array_api.asarray(arr, dtype=array_api.float32)

    arr = np.arange(5, dtype=np.int32)
    np.testing.assert_array_equal(asarray_dtype(5).execute(), arr)


@pytest.mark.parametrize('start', (3, 3.0, 0, 1, 4, 5.5, -3))
def test_arange1(heavydb, start):
    heavydb.unregister()

    @heavydb('T[](T)', T=['int64', 'float64'])
    def arange1(start):
        return array_api.arange(start)

    if start <= 0:
        _, r = heavydb.sql_execute(f'select arange1({start})')
        assert list(r) == [(None,)]
    else:
        got = arange1(start).execute()
        expected = np.arange(start)
        np.testing.assert_array_equal(expected, got)


@pytest.mark.parametrize('start, stop', [
    (3, 7),
    (3.0, 7),
    (3, 7.0),
    (3.0, 7.0),
    (-1, 5),
    (-8, -1),
    (0.5, 4),
])
def test_arange2(heavydb, start, stop):
    heavydb.unregister()

    @heavydb('T[](T, T)', T=['int64', 'float64'])
    def arange2(start, stop):
        return array_api.arange(start, stop)

    got = arange2(start, stop).execute()
    expected = np.arange(start, stop)
    np.testing.assert_array_equal(expected, got)


@pytest.mark.parametrize('start, stop, step', [
    (0, 5, 1),
    (-8, -1, 3),
    (0, -10, -2),
    (0.5, 4, 2),
    (0, 1, 0.1),
])
def test_arange3(heavydb, start, stop, step):
    heavydb.unregister()

    @heavydb('T[](T, T, T)', T=['int64', 'float64'])
    def arange3(start, stop, step):
        return array_api.arange(start, stop, step)

    got = arange3(start, stop, step).execute()
    expected = np.arange(start, stop, step)
    np.testing.assert_allclose(expected, got)


@pytest.mark.parametrize('start, dtype', [
    (5, 'float64'),
    (2.0, 'int32'),
    (10, 'int8'),
])
def test_arange1_dtype(heavydb, start, dtype):
    heavydb.unregister()

    @heavydb(f'{dtype}[](T)', T=['int64', 'float64'])
    def arange1_dtype(start):
        return array_api.arange(start, dtype=dtype)

    got = arange1_dtype(start).execute()
    expected = np.arange(start, dtype=np.dtype(dtype))
    np.testing.assert_allclose(expected, got)


@pytest.mark.parametrize('start, stop, dtype', [
    (1, 5, 'float32'),
    (2.0, 8, 'int32'),
    (-2, 10, 'int8'),
])
def test_arange2_dtype(heavydb, start, stop, dtype):
    heavydb.unregister()

    @heavydb(f'{dtype}[](T, T)', T=['int64', 'float64'])
    def arange2_dtype(start, stop):
        return array_api.arange(start, stop, dtype=dtype)

    got = arange2_dtype(start, stop).execute()
    expected = np.arange(start, stop, dtype=np.dtype(dtype))
    np.testing.assert_allclose(expected, got)


@pytest.mark.parametrize('start, stop, step, dtype', [
    (0, 5, 1, 'float64'),
    (-8, -1, 3, 'int32'),
    (0, -10, -2, 'float32'),
])
def test_arange3_dtype(heavydb, start, stop, step, dtype):
    heavydb.unregister()

    @heavydb(f'{dtype}[](T, T, T)', T=['int64'])
    def arange3_dtype(start, stop, step):
        return array_api.arange(start, stop, step, dtype)

    got = arange3_dtype(start, stop, step).execute()
    expected = np.arange(start, stop, step, np.dtype(dtype))
    np.testing.assert_allclose(expected, got)
