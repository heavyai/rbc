import itertools
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


dtypes = ('bool', 'int8', 'int16', 'int32', 'int64', 'float32', 'float64')


def define(heavydb):
    @heavydb('T[](T[])', T=list(dtypes))
    def flip(x):
        return array_api.flip(x)


@pytest.mark.parametrize('dtype', dtypes)
def test_flip(heavydb, dtype):
    table = heavydb.table_name + 'arraynullrepeat'
    col = dict(float64='f8', float32='f4', int64='i8', int32='i4',
               int16='i2', int8='i1', bool='b')[dtype]
    query = f'select {col}, flip({col}) from {table}'
    _, r = heavydb.sql_execute(query)

    for val, got in r:
        if val == [] or val is None:
            assert got is None
        else:
            np.testing.assert_array_equal(np.flip(val), got)


@pytest.mark.parametrize('args', itertools.combinations(dtypes, r=3))
def test_concat(heavydb, args):
    heavydb.unregister()

    sz = len(args)
    m = ', '.join(map(lambda x: f'{x}[]', args))
    ta = ', '.join([f't{i}' for i in range(sz)])
    dt = np.result_type(*args)
    fn = textwrap.dedent(f'''
        @heavydb("{dt}[]({m})")
        def concat({ta}):
            return array_api.concat({ta})
    ''')
    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    table = heavydb.table_name + 'arraynullrepeat'
    d = dict(float64='f8', float32='f4', int64='i8', int32='i4',
             int16='i2', int8='i1', bool='b')
    c = ', '.join([d.get(arg) for arg in args])
    _, r = heavydb.sql_execute(f'SELECT {c}, concat({c}) FROM {table}')

    for val in r:
        x, r = val[:-1], val[-1]
        lst = []
        for v in x:
            if v is not None:
                lst += v
        if lst == []:
            assert r is None
        else:
            assert lst == r
