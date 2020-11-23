import pytest
import numpy as np
import rbc.omnisci_backend as omni
from rbc.utils import get_version
if get_version('numba') >= (0, 49):
    from numba.core import types
else:
    from numba import types


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = 'rbc_test_omnisci_array'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    sqltypes = ['FLOAT[]', 'DOUBLE[]',
                'TINYINT[]', 'SMALLINT[]', 'INT[]', 'BIGINT[]',
                'BOOLEAN[]']
    # todo: TEXT ENCODING DICT, TEXT ENCODING NONE, TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://www.omnisci.com/docs/latest/5_datatypes.html
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
        m.sql_execute(
            'INSERT INTO {table_name} VALUES ({table_row})'.format(**locals()))
    m.table_name = table_name
    yield m
    try:
        m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


def np_ones(sz):
    return omni.ones(sz, types.int32)


def np_ones_dtype(sz):
    return omni.ones(sz)


def np_ones_like_dtype(i4):
    return omni.ones_like(i4, dtype=types.double)


def np_ones_like(i4):
    return omni.ones_like(i4)


def np_zeros(sz):
    return omni.zeros(sz, np.int32)


def np_zeros_dtype(sz):
    return omni.zeros(sz)


def np_zeros_like(i4):
    return omni.zeros_like(i4)


def np_zeros_like_dtype(i4):
    return omni.zeros_like(i4, dtype=types.double)


def np_full(sz, fill_value):
    return omni.full(sz, fill_value, types.double)


def np_full_dtype(sz, fill_value):
    return omni.full(sz, fill_value)


def np_full_like(i1, fill_value):
    return omni.full_like(i1, fill_value)


def np_full_like_dtype(i1, fill_value):
    return omni.full_like(i1, fill_value, dtype=types.double)


def np_cumsum(sz):
    a = omni.ones(sz)
    return omni.cumsum(a)


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
    ('cumsum', 'double[](int32)', (5,), np.arange(1, 6)),
]


@pytest.mark.parametrize("method, signature, args, expected", array_methods,
                         ids=[item[0] for item in array_methods])
def test_array_methods(omnisci, method, signature, args, expected):
    if (available_version[:3] == (5, 3, 1)
        and method in ['full', 'full_dtype', 'ones', 'ones_dtype', 'zeros',
                       'zeros_dtype', 'cumsum']):
        pytest.skip(
            f'{method}: crashes CPU-only omniscidb server v 5.4 [issue 113]')
    omnisci.reset()

    fn = omnisci(signature)(eval('np_{}'.format(method)))

    query = 'select np_{method}'.format(**locals()) + \
            '(' + ', '.join(map(str, args)) + ')' + \
            ' from {omnisci.table_name};'.format(**locals())

    _, result = omnisci.sql_execute(query)
    out = list(result)[0]

    assert np.array_equal(expected, out[0]), 'np_' + method
