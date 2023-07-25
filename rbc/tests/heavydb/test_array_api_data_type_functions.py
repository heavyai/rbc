import pytest
import textwrap
import numpy as np
import itertools
from rbc.tests import heavydb_fixture
from rbc.stdlib import array_api


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['arraynullrepeat']):
        yield o


@pytest.mark.parametrize('datatype', ('float64', 'float32'))
def test_finfo(heavydb, datatype):
    heavydb.unregister()

    fn: str = textwrap.dedent(f'''
        @heavydb('{datatype}[](int64)')
        def finfo(_dummy):
            info = array_api.finfo(array_api.{datatype})
            a = array_api.zeros(5, array_api.{datatype})
            a[0] = info.bits
            a[1] = info.eps
            a[2] = info.min
            a[3] = info.max
            a[4] = info.smallest_normal
            return a
    ''')

    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    _, r = heavydb.sql_execute('select finfo(0);')
    result = list(r)[0][0]
    assert result[0] == np.finfo(datatype).bits
    assert np.isclose(result[1], np.finfo(datatype).resolution)
    assert result[2] == np.finfo(datatype).min
    assert result[3] == np.finfo(datatype).max


@pytest.mark.parametrize('datatype', ('int64', 'int32', 'int16', 'int8'))
def test_iinfo(heavydb, datatype):
    heavydb.unregister()

    fn: str = textwrap.dedent(f'''
        @heavydb('int64[](int64)')
        def iinfo(_dummy):
            info = array_api.iinfo(array_api.{datatype})
            a = array_api.zeros(3, array_api.int64)
            a[0] = info.bits
            a[1] = info.min
            a[2] = info.max
            return a
    ''')

    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    _, r = heavydb.sql_execute('select iinfo(0);')
    result = list(r)[0][0]
    assert result[0] == np.iinfo(datatype).bits
    assert result[1] == np.iinfo(datatype).min + 1
    assert result[2] == np.iinfo(datatype).max


dtypes = ('bool', 'int8', 'int16', 'int32', 'int64', 'float32', 'float64')


@pytest.mark.parametrize('from_', dtypes)
@pytest.mark.parametrize('to', dtypes)
def test_can_cast(heavydb, from_, to):
    heavydb.unregister()

    fn = textwrap.dedent(f'''
        @heavydb('bool(int32)')
        def can_cast(_dummy):
            return array_api.can_cast(array_api.{from_}, array_api.{to})
    ''')

    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    _, r = heavydb.sql_execute('select can_cast(0);')
    dt_from, dt_to = np.dtype(from_), np.dtype(to)
    assert list(r)[0][0] == np.can_cast(dt_from, dt_to)


@pytest.mark.parametrize('fromty', dtypes)
@pytest.mark.parametrize('toty', dtypes)
def test_astype(heavydb, fromty, toty):
    heavydb.unregister()

    fn = textwrap.dedent(f'''
        @heavydb("{toty}[]({fromty}[])")
        def astype(x):
            return array_api.astype(x, array_api.{toty})
    ''')
    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    table = heavydb.table_name + 'arraynullrepeat'
    col = dict(float64='f8', float32='f4', int64='i8', int32='i4',
               int16='i2', int8='i1', bool='b').get(fromty)
    query = f'SELECT {col}, astype({col}) FROM {table}'
    _, r = heavydb.sql_execute(query)

    for val, got in r:
        if val == [] or val is None:
            assert got is None
        else:
            assert len(val) == len(got)
            for u, v in zip(val, got):
                if u is None:
                    assert v is None
                else:
                    dt = np.dtype(toty).type
                    assert dt(u) == dt(v)


@pytest.mark.parametrize('args', itertools.combinations(dtypes, r=2))
def test_result_type(heavydb, args):
    heavydb.unregister()

    m = ', '.join(map(lambda x: f'array_api.{x}', args))
    fn = textwrap.dedent(f'''
        @heavydb("TextEncodingNone(int64)")
        def result_type(dummy):
            t = array_api.result_type({m})
            return TextEncodingNone(str(t))
    ''')
    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    r = heavydb.get_caller('result_type')(0).execute()
    assert np.dtype(r) == np.result_type(*args)


@pytest.mark.parametrize('args', itertools.combinations(dtypes, r=3))
def test_result_type_array(heavydb, args):
    heavydb.unregister()

    sz = len(args)
    m = ', '.join(map(lambda x: f'{x}[]', args))
    ta = ', '.join([f't{i}' for i in range(sz)])
    fn = textwrap.dedent(f'''
        @heavydb("TextEncodingNone({m})")
        def result_type({ta}):
            t = array_api.result_type({ta})
            return TextEncodingNone(str(t))
    ''')
    lc = {'heavydb': heavydb}
    exec(fn, {'array_api': array_api}, lc)

    table = heavydb.table_name + 'arraynullrepeat'
    d = dict(float64='f8', float32='f4', int64='i8', int32='i4',
             int16='i2', int8='i1', bool='b')
    c = ', '.join([d.get(arg) for arg in args])
    _, r = heavydb.sql_execute(f'SELECT result_type({c}) FROM {table} LIMIT 1')
    assert list(r)[0][0] == np.result_type(*args)
