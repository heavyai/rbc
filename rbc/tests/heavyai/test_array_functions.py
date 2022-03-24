import pytest
import numpy as np

from rbc.stdlib import array_api
from rbc.typesystem import Type
from rbc.heavydb import type_to_type_name
from numba.core import types


rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    # TODO: use heavydb_fixture from rbc/tests/__init__.py
    config = rbc_heavydb.get_client_config(debug=not True)
    m = rbc_heavydb.RemoteHeavyDB(**config)
    table_name = 'rbc_test_heavydb_array'

    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    sqltypes = ['FLOAT[]', 'DOUBLE[]',
                'TINYINT[]', 'SMALLINT[]', 'INT[]', 'BIGINT[]',
                'BOOLEAN[]']
    # todo: TEXT ENCODING DICT, TEXT ENCODING NONE, TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://docs.heavy.ai/sql/data-definition-ddl/datatypes-and-fixed-encoding
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});'
        .format(**locals()))

    def row_value(row, col, colname):
        if colname == 'b':
            return 'ARRAY[%s]' % (', '.join(
                ("'true'" if i % 2 == 0 else "'false'")
                for i in range(-3, 3)))
        if colname.startswith('f'):
            return 'ARRAY[%s]' % (', '.join(
                str(row * 10 + i + 0.5) for i in range(-3, 3)))
        return 'ARRAY[%s]' % (', '.join(
            str(row * 10 + i) for i in range(-3, 3)))

    rows = 5
    for i in range(rows):
        table_row = ', '.join(str(row_value(i, j, n))
                              for j, n in enumerate(colnames))
        m.sql_execute(f'INSERT INTO {table_name} VALUES ({table_row})')
    m.table_name = table_name
    yield m
    try:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


def np_ones(sz):
    return array_api.ones(sz, dtype=types.int32)


def np_ones_dtype(sz):
    return array_api.ones(sz)


def np_ones_like_dtype(i4):
    return array_api.ones_like(i4, dtype=types.double)


def np_ones_like(i4):
    return array_api.ones_like(i4)


def np_empty(sz):
    return array_api.empty(sz, np.int32)


def np_empty_dtype(sz):
    return array_api.empty(sz)


def np_empty_like(i4):
    return array_api.empty_like(i4)


def np_zeros(sz):
    return array_api.zeros(sz, np.int32)


def np_zeros_dtype(sz):
    return array_api.zeros(sz)


def np_zeros_like(i4):
    return array_api.zeros_like(i4)


def np_zeros_like_dtype(i4):
    return array_api.zeros_like(i4, dtype=types.double)


def np_full(sz, fill_value):
    return array_api.full(sz, fill_value, dtype=types.double)


def np_full_dtype(sz, fill_value):
    return array_api.full(sz, fill_value)


def np_full_like(i1, fill_value):
    return array_api.full_like(i1, fill_value)


def np_full_like_dtype(i1, fill_value):
    return array_api.full_like(i1, fill_value, dtype=types.double)


def np_cumsum(sz):
    a = array_api.ones(sz)
    return array_api.cumsum(a)


array_methods = [
    ('full', 'double[](int64, double)', (5, 3), np.full(5, 3, dtype=np.int32)),
    ('full_dtype', 'double[](int64, double)', (5, 3), np.full(5, 3)),
    ('full_like', 'int8[](int8[], int32)', ('i1', 3), np.full(6, 3, dtype='b')),  # noqa: E501
    ('full_like_dtype', 'double[](int8[], double)', ('i1', 3.0), np.full(6, 3, dtype='q')),  # noqa: E501
    ('ones', 'int32[](int64)', (5,), np.ones(5, dtype=np.int32)),
    ('ones_dtype', 'double[](int64)', (5,), np.ones(5)),
    ('ones_like', 'int32[](int32[])', ('i4',), np.ones(6, dtype='i')),
    ('ones_like_dtype', 'double[](int32[])', ('i4',), np.ones(6, dtype='q')),
    ('zeros', 'int32[](int64)', (5,), np.zeros(5, dtype=np.int32)),
    ('zeros_like', 'int32[](int32[])', ('i4',), np.zeros(6, dtype='i')),
    ('zeros_like_dtype', 'double[](int32[])', ('i4',), np.zeros(6, dtype='q')),
    ('zeros_dtype', 'double[](int64)', (5,), np.zeros(5)),
    ('empty', 'int32[](int64)', (5,), np.empty(5, dtype=np.int32)),
    ('empty_like', 'int32[](int32[])', ('i4',), np.empty(6, dtype='i')),
    ('empty_dtype', 'double[](int64)', (5,), np.empty(5)),
    ('cumsum', 'double[](int32)', (5,), np.arange(1, 6)),
]


@pytest.mark.parametrize("method, signature, args, expected", array_methods,
                         ids=[item[0] for item in array_methods])
def test_array_methods(heavydb, method, signature, args, expected):
    heavydb.reset()

    fn = heavydb(signature)(eval('np_{}'.format(method)))

    query = 'select np_{method}'.format(**locals()) + \
            '(' + ', '.join(map(str, args)) + ')' + \
            ' from {heavydb.table_name};'.format(**locals())

    _, result = heavydb.sql_execute(query)
    out = list(result)[0]

    if 'empty' in method:
        assert out == ([None] * len(expected),)
    else:
        assert np.array_equal(expected, out[0]), 'np_' + method


@pytest.mark.parametrize('col', ('i4', 'i8', 'f4'))
def test_dtype_attribute(heavydb, col):
    heavydb.reset()

    @heavydb('T[](T[])', T=['int32', 'int64', 'float32'], devices=['cpu'])
    def zeros(x):
        z = array_api.zeros(len(x), x.dtype)
        return z

    query = f'select zeros({col}) from {heavydb.table_name} limit 1;'
    _, result = heavydb.sql_execute(query)
    assert np.all(list(result)[0][0] == np.zeros(6, dtype=col))


dtype_to_col = dict(int8_t='i1', int16_t='i2', int32_t='i4',
                    int64_t='i8', float='f4', double='f8')
functions = ('zeros', 'zeros_like',
             'full', 'full_like',
             'empty', 'empty_like',
             'ones', 'ones_like',)


@pytest.mark.parametrize('dtype', ('int32_t', 'float'))  # test all dtypes?
@pytest.mark.parametrize('func_name', functions)
def test_literal_string_dtype(heavydb, dtype, func_name):
    heavydb.reset()

    func = getattr(array_api, func_name)

    if func_name == 'full_like':
        fill_value = 0.0 if dtype in ('float', 'double') else 0

        @heavydb('T[](T[])', T=[dtype], devices=['cpu'])
        def fn(arr):
            return func(arr, fill_value, dtype=dtype)
        query = f'select fn({dtype_to_col[dtype]}) from {heavydb.table_name} limit 1;'
    elif func_name.endswith('_like'):

        @heavydb('T[](T[])', T=[dtype], devices=['cpu'])
        def fn(arr):
            return func(arr, dtype=dtype)
        query = f'select fn({dtype_to_col[dtype]}) from {heavydb.table_name} limit 1;'
    elif func_name == 'full':

        @heavydb('T[](int32, T)', T=[dtype], devices=['cpu'])
        def fn(shape, fill_value):
            return func(shape, fill_value, dtype)
        query = f'{str(fn(5, 0))} limit 1;'
    else:

        @heavydb('T[](int32)', T=[dtype], devices=['cpu'])
        def fn(sz):
            z = func(sz, dtype)
            return z
        query = 'select fn(5) limit 1;'

    _, result = heavydb.sql_execute(query)
    # if the execution succeed, it means the return array has type specified by dtype
    assert len(list(result)[0][0]) > 0


dtypes = ('int8_t', 'int16_t', 'int32_t', 'int64_t', 'float', 'double')


@pytest.mark.parametrize('dtype', dtypes)
@pytest.mark.parametrize('func_name', ('full', 'full_like'))
def test_detect_dtype_from_fill_value(heavydb, dtype, func_name):
    heavydb.reset()

    if func_name == 'full':

        @heavydb('T[](int32, T)', T=[dtype], devices=['cpu'])
        def fn(shape, fill_value):
            return array_api.full(shape, fill_value)

        query = str(fn(5, 3))
    else:  # full_like

        @heavydb('T[](T[], T)', T=[dtype], devices=['cpu'])
        def fn(arr, fill_value):
            return array_api.full_like(arr, fill_value)

        datum_type = type_to_type_name(Type.fromstring(dtype))
        query = (f'SELECT fn({dtype_to_col[dtype]}, CAST(3 as {datum_type})) '
                 f'FROM {heavydb.table_name}')

    _, result = heavydb.sql_execute(f'{query} limit 1;')
    # if the execution succeed, it means the return array has type specified by fill_value
    assert len(list(result)[0][0]) > 0
