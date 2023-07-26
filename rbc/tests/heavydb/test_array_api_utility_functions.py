import pytest
import textwrap
import numpy as np
from rbc.tests import heavydb_fixture
from rbc.stdlib import array_api


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['arraynullrepeat']):
        yield o


@pytest.mark.parametrize('func', ('any', 'all'))
@pytest.mark.parametrize('dtype', ('float64', 'float32', 'int64', 'int32',
                                   'int16', 'int8', 'bool'))
def test_any_all(heavydb, func, dtype):
    heavydb.unregister()

    fn = textwrap.dedent(f'''
        @heavydb("bool({dtype}[])")
        def rbc_{func}(arr):
            return array_api.{func}(arr)
    ''')
    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    table = heavydb.table_name + 'arraynullrepeat'
    col = dict(float64='f8', float32='f4', int64='i8', int32='i4',
               int16='i2', int8='i1', bool='b')[dtype]

    _, r = heavydb.sql_execute(f'SELECT {col}, rbc_{func}({col}) from {table}')
    for val, got in r:
        if val == [] or val is None:
            assert got == dict(any=False, all=True)[func], val
        else:
            x = np.asarray(val)
            x = np.asarray(x[x != None], dtype=dtype)  # noqa: E711
            assert getattr(np, func)(x) == got
