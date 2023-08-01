import textwrap

import numpy as np
import pytest

from rbc.stdlib import array_api
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['arraynullrepeat']):
        define(o)
        yield o


def define(heavydb):
    T = ['double', 'float', 'int64', 'int32', 'int16', 'int8']

    @heavydb('int64[](T[])', T=T)
    def nonzero(arr):
        return array_api.nonzero(arr)

    @heavydb('int64[](T[])', T=T)
    def argmin(arr):
        return array_api.argmin(arr)

    @heavydb('int64[](T[])', T=T)
    def argmax(arr):
        return array_api.argmax(arr)

    @heavydb('T[](T[])', T=T)
    def rbc_where(a):
        cond = []
        for i in range(len(a)):
            cond.append(True if i % 2 else False)
        return array_api.where(cond, a, array_api.flip(a))


@pytest.mark.parametrize('fn', ('nonzero', 'argmin', 'argmax'))
@pytest.mark.parametrize('col', ('f8', 'f4', 'i8', 'i4', 'i2'))
def test_nonzero_argmin_argmax(heavydb, fn, col):

    table = heavydb.table_name + 'arraynullrepeat'
    query = f"SELECT {col}, {fn}({col}) FROM {table}"
    _, r = heavydb.sql_execute(query)
    for val, got in r:
        if val is None or val == []:
            assert got is None
        else:
            x = np.asarray(val)
            if fn == 'nonzero':
                x[x == None] = 0  # noqa: E711
                expected = x.nonzero()[0]
            elif fn == 'argmin':
                x[x == None] = 1234  # noqa: E711
                expected = x.argmin()
            else:
                x[x == None] = -123  # noqa: E711
                expected = x.argmax()

            np.testing.assert_array_equal(expected, got)


dtypes = ('bool', 'int8', 'int16', 'int32', 'int64', 'float32', 'float64')


@pytest.mark.parametrize('typ', dtypes)
def test_where(heavydb, typ):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Test requires HeavyDB 6.2 or newer')

    table = heavydb.table_name + 'arraynullrepeat'
    c = dict(float64='f8', float32='f4', int64='i8', int32='i4',
             int16='i2', int8='i1', bool='b')[typ]
    _, r = heavydb.sql_execute(f'SELECT {c}, rbc_where({c}) FROM {table}')

    for val, got in r:
        if val == [] or val is None:
            assert got is None
        else:
            cond = []
            for i in range(len(val)):
                cond.append(True if i % 2 else False)
            expected = np.where(cond, val, np.flip(val))
            np.testing.assert_array_equal(expected, got)


@pytest.mark.parametrize('dtypes', [
    ('int16', 'int64'),
    ('int64', 'int16'),
    ('float64', 'int16'),
    ('float64', 'int64'),
    ('int16', 'float64'),
    ('int64', 'float64'),
])
def test_where_diff(heavydb, dtypes):
    if heavydb.version[:2] < (6, 2):
        pytest.skip('Test requires HeavyDB 6.2 or newer')

    heavydb.unregister()

    d = dict(float64='f8', float32='f4', int64='i8', int32='i4',
             int16='i2', int8='i1', bool='b')
    a, b = dtypes
    c1, c2 = d[a], d[b]

    table = heavydb.table_name + 'arraynullrepeat'

    dt = np.result_type(a, b)
    fn = textwrap.dedent(f'''
        @heavydb("{dt}[]({a}[], {b}[])")
        def rbc_where(a, b):
            if len(a) != len(b):
                return Array(0, array_api.{dt})
            cond = []
            for i in range(len(a)):
                cond.append(True if i % 2 else False)
            return array_api.where(cond, a, b)
    ''')
    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    d = dict(float64='f8', float32='f4', int64='i8', int32='i4',
             int16='i2', int8='i1', bool='b')
    c1, c2 = d[a], d[b]

    table = heavydb.table_name + 'arraynullrepeat'
    query = (f"SELECT {c1}, {c2}, rbc_where({c1}, {c2}) FROM {table} WHERE "
             f"ARRAY_LENGTH({c1}) = ARRAY_LENGTH({c2})")
    _, r = heavydb.sql_execute(query)

    for x1, x2, got in r:
        if x1 == []:
            assert x2 == []
            assert got is None
        else:
            cond = []
            for i in range(len(x1)):
                cond.append(True if i % 2 else False)
            expected = np.where(cond, x1, x2)
            np.testing.assert_array_equal(expected, got)
