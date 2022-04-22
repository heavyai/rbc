import os
from collections import defaultdict

import numpy as np

from rbc.heavydb import Array
from rbc.externals.stdio import printf
from rbc.errors import HeavyDBServerError
from rbc.stdlib import array_api
from numba import types as nb_types
from numba import TypingError
import pytest

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    # TODO: use heavydb_fixture from rbc/tests/__init__.py
    config = rbc_heavydb.get_client_config(debug=not True)
    m = rbc_heavydb.RemoteHeavyDB(**config)
    table_name = os.path.splitext(os.path.basename(__file__))[0]

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
    m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});')

    data = defaultdict(list)
    for i in range(5):
        for j, n in enumerate(colnames):
            if n == 'b':
                data[n].append([_i % 2 == 0 for _i in range(-3, 3)])
            elif n.startswith('f'):
                data[n].append([i * 10 + _i + 0.5 for _i in range(-3, 3)])
            else:
                data[n].append([i * 10 + _i for _i in range(-3, 3)])

    m.load_table_columnar(table_name, **data)
    m.table_name = table_name
    yield m
    try:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


@pytest.mark.parametrize('c_name', ['int8_t i1', 'int16_t i2', 'int32_t i4', 'int64_t i8',
                                    'float f4', 'double f8'])
@pytest.mark.parametrize('device', ['cpu', 'gpu'])
def test_ptr(heavydb, c_name, device):
    heavydb.reset()
    if not heavydb.has_cuda and device == 'gpu':
        pytest.skip('test requires CUDA-enabled heavydb server')
    from rbc.external import external

    if heavydb.compiler is None:
        pytest.skip('test requires clang C/C++ compiler')

    ctype, cname = c_name.split()

    c_code = f'''
#include <stdint.h>
#ifdef __cplusplus
extern "C" {{
#endif
{ctype} mysum_impl({ctype}* x, int n) {{
    {ctype} r = 0;
    for (int i=0; i < n; i++) {{
      r += x[i];
    }}
    return r;
}}

{ctype} myval_impl({ctype}* x) {{
    return *x;
}}
#ifdef __cplusplus
}}
#endif
    '''
    heavydb.user_defined_llvm_ir[device] = heavydb.compiler(c_code)
    mysum_impl = external(f'{ctype} mysum_impl({ctype}*, int32_t)')
    myval_impl = external(f'{ctype} myval_impl({ctype}*)')

    @heavydb(f'{ctype}({ctype}[])', devices=[device])
    def mysum_ptr(x):
        return mysum_impl(x.ptr(), len(x))

    @heavydb(f'{ctype}({ctype}[], int32_t)', devices=[device])
    def myval_ptr(x, i):
        return myval_impl(x.ptr(i))

    desrc, result = heavydb.sql_execute(
        f'select {cname}, mysum_ptr({cname}) from {heavydb.table_name}')
    for a, r in result:
        if cname == 'i1':
            assert sum(a) % 256 == r % 256
        else:
            assert sum(a) == r

    desrc, result = heavydb.sql_execute(
        f'select {cname}, myval_ptr({cname}, 0), myval_ptr({cname}, 2) from {heavydb.table_name}')
    for a, r0, r2 in result:
        assert a[0] == r0
        assert a[2] == r2


def test_len_i32(heavydb):
    heavydb.reset()

    @heavydb('int64(int32[])')
    def array_sz_int32(x):
        return len(x)
    desrc, result = heavydb.sql_execute(
        f'select i4, array_sz_int32(i4) from {heavydb.table_name}')
    for a, sz in result:
        assert len(a) == sz


def test_len_f64(heavydb):
    heavydb.reset()

    @heavydb('int64(float64[])')
    def array_sz_double(x):
        return len(x)

    desrc, result = heavydb.sql_execute(
        f'select f8, array_sz_double(f8) from {heavydb.table_name}')
    for a, sz in result:
        assert len(a) == sz


@pytest.mark.skipif(available_version[:2] == (5, 1),
                    reason="skip due to a bug in heavydb 5.1 (got %s)" % (
                        available_version,))
def test_getitem_bool(heavydb):
    heavydb.reset()

    @heavydb('bool(bool[], int64)')
    def array_getitem_bool(x, i):
        return x[i]

    query = f'select b, array_getitem_bool(b, 2) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_i8(heavydb):
    heavydb.reset()

    @heavydb('int8(int8[], int32)')
    def array_getitem_int8(x, i):
        return x[i]

    query = f'select i1, array_getitem_int8(i1, 2) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_i32(heavydb):
    heavydb.reset()

    @heavydb('int32(int32[], int32)')
    def array_getitem_int32(x, i):
        return x[i]

    query = f'select i4, array_getitem_int32(i4, 2) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_i64(heavydb):
    heavydb.reset()

    @heavydb('int64(int64[], int64)')
    def array_getitem_int64(x, i):
        return x[i]

    query = f'select i8, array_getitem_int64(i8, 2) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_float(heavydb):
    heavydb.reset()

    @heavydb('double(double[], int32)')
    def array_getitem_double(x, i):
        return x[i]

    query = f'select f8, array_getitem_double(f8, 2) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, item in result:
        assert a[2] == item
        assert type(a[2]) == type(item)

    @heavydb('float(float[], int64)')
    def array_getitem_float(x, i):
        return x[i]

    query = f'select f4, array_getitem_float(f4, 2) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, item in result:
        assert a[2] == item
        assert type(a[2]) == type(item)


def test_sum(heavydb):
    heavydb.reset()

    @heavydb('int32(int32[])')
    def array_sum_int32(x):
        r = 0
        n = len(x)
        for i in range(n):
            r = r + x[i]
        return r

    query = f'select i4, array_sum_int32(i4) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for a, s in result:
        assert sum(a) == s


@pytest.mark.skipif(available_version[:2] == (5, 1),
                    reason="skip due to a bug in heavydb 5.1 (got %s)" % (
                        available_version,))
def test_even_sum(heavydb):
    heavydb.reset()

    @heavydb('int32(bool[], int32[])')
    def array_even_sum_int32(b, x):
        r = 0
        n = len(x)
        for i in range(n):
            if b[i]:
                r = r + x[i]
        return r

    query = f'select b, i4, array_even_sum_int32(b, i4) from {heavydb.table_name}'
    desrc, result = heavydb.sql_execute(query)
    for b, i4, s in result:
        assert sum([i_ for b_, i_ in zip(b, i4) if b_]) == s


def test_array_setitem(heavydb):
    heavydb.reset()

    @heavydb('double(double[], int32)')
    def array_setitem_sum(b, c):
        n = len(b)
        s = 0
        for i in range(n):
            b[i] = b[i] * c  # changes the value inplace
            s += b[i]
            b[i] = b[i] / c
        return s

    query = f'select f8, array_setitem_sum(f8, 4) from {heavydb.table_name}'
    _, result = heavydb.sql_execute(query)

    for f8, s in result:
        assert sum(f8) * 4 == s


def test_array_constructor_noreturn(heavydb):
    heavydb.reset()

    @heavydb('float64(int32)')
    def array_noreturn(size):
        a = Array(size, nb_types.float64)
        b = Array(size, nb_types.float64)
        c = Array(size, nb_types.float64)
        for i in range(size):
            a[i] = b[i] = c[i] = i + 3.0
        s = 0.0
        for i in range(size):
            s += a[i] + b[i] + c[i] - a[i] * b[i]
        return s

    query = 'select array_noreturn(10)'
    _, result = heavydb.sql_execute(query)
    r = list(result)[0]
    assert (r == (-420.0,))


def test_array_constructor_return(heavydb):
    heavydb.reset()

    @heavydb('float64[](int32)')
    def array_return(size):
        printf("entering array_return(%i)\n", size)
        a = Array(size, nb_types.float64)
        b = Array(size, nb_types.float64)
        for i in range(size):
            a[i] = float(i)
            b[i] = float(size - i - 1)
        if size % 2:
            c = a
        else:
            c = b
        printf("returning array with length %i\n", len(c))
        return c

    query = 'select array_return(9), array_return(10)'
    _, result = heavydb.sql_execute(query)

    r = list(result)[0]
    assert r == (list(map(float, range(9))),
                 list(map(float, reversed(range(10)))))


def test_array_constructor_len(heavydb):
    heavydb.reset()

    @heavydb('int64(int32)')
    def array_len(size):
        a = Array(size, nb_types.float64)
        return len(a)

    query = 'select array_len(30)'
    _, result = heavydb.sql_execute(query)

    assert list(result)[0] == (30,)


def test_array_constructor_getitem(heavydb):
    heavydb.reset()

    @heavydb('double(int32, int32)')
    def array_ptr(size, pos):
        a = Array(size, np.double)
        for i in range(size):
            a[i] = i + 0.0
        return a[pos]

    query = 'select array_ptr(5, 3)'
    _, result = heavydb.sql_execute(query)

    assert list(result)[0] == (3.0,)


def test_array_constructor_is_null(heavydb):
    heavydb.reset()

    @heavydb('int8(int64)')
    def array_is_null(size):
        a = Array(size, 'double')
        return a.is_null()

    query = 'select array_is_null(3);'
    _, result = heavydb.sql_execute(query)

    assert list(result)[0] == (0,)


inps = [('int32', 'i4', 'trunc'), ('int32', 'i4', 'sext'),
        ('int32', 'i4', 'zext'), ('float', 'f4', 'fptrunc'),
        ('double', 'f8', 'fpext')]


@pytest.mark.parametrize("typ, col, suffix", inps,
                         ids=[item[-1] for item in inps])
def test_issue197(heavydb, typ, col, suffix):
    heavydb.reset()

    cast = dict(
        trunc=nb_types.int64,
        sext=nb_types.int8,
        zext=nb_types.uint8,
        fptrunc=nb_types.float64,
        fpext=nb_types.float32)[suffix]

    def fn_issue197(x):
        y = array_api.zeros_like(x)
        for i in range(len(x)):
            y[i] = cast(x[i] + 3)
        return y

    fn_name = f"fn_issue197_{typ}_{suffix}"
    fn_issue197.__name__ = fn_name

    heavydb(f'{typ}[]({typ}[])')(fn_issue197)

    _, result = heavydb.sql_execute(
        f'SELECT {col}, {fn_name}({col}) FROM {heavydb.table_name};'
    )

    column, ret = list(result)[0]
    for x, y in zip(column, ret):
        assert y == x + 3


def test_issue197_bool(heavydb):
    heavydb.reset()

    @heavydb('bool[](bool[])')
    def fn_issue197_bool(x):
        y = array_api.zeros_like(x)
        for i in range(len(x)):
            y[i] = bool(x[i])
        return y

    col = 'b'
    fn_name = 'fn_issue197_bool'

    _, result = heavydb.sql_execute(
        f'SELECT {col}, {fn_name}({col}) FROM {heavydb.table_name};'
    )

    column, ret = list(result)[0]
    for x, y in zip(column, ret):
        assert bool(x) == bool(y)


def test_issue109(heavydb):

    @heavydb('double[](int32)')
    def issue109(size):
        a = Array(5, 'double')
        for i in range(5):
            a[i] = nb_types.double(i)
        return a

    _, result = heavydb.sql_execute('select issue109(3);')
    assert list(result) == [([0.0, 1.0, 2.0, 3.0, 4.0],)]


def test_issue77(heavydb):

    @heavydb('int64[]()')
    def issue77():
        a = Array(5, 'int64')
        a.fill(1)
        return a

    if heavydb.version[:2] >= (5, 8):
        _, result = heavydb.sql_execute('select issue77();')
        assert list(result)[0][0] == [1, 1, 1, 1, 1]
    else:
        with pytest.raises(HeavyDBServerError) as exc:
            _, result = heavydb.sql_execute('select issue77();')

        assert exc.match('Could not bind issue77()')


def test_array_dtype(heavydb):
    table = heavydb.table_name

    @heavydb('T(T[])', T=['int32', 'int64'])
    def array_dtype_fn(x):
        if x.dtype == nb_types.int32:
            return 32
        else:
            return 64

    for col, r in (('i4', 32), ('i8', 64)):
        _, result = heavydb.sql_execute(f'select array_dtype_fn({col}) from {table}')
        assert list(result) == [(r,)] * 5


def test_array_invalid_dtype(heavydb):

    @heavydb('int64[](int64)')
    def array_invalid_dtype(sz):
        return array_api.zeros(sz, dtype=123)

    err_msg = ("Expected dtype derived from numba.types.DTypeSpec but got "
               "<class 'numba.core.types.scalars.Integer'>")
    with pytest.raises(TypingError, match=err_msg):
        heavydb.register()
    heavydb.reset()


def test_array_enumerate(heavydb):
    table = heavydb.table_name

    @heavydb('T(T[])', T=['int32'])
    def array_enumerate(x):
        s = 0
        for i, e in enumerate(x):
            s += e
        return s

    _, result = heavydb.sql_execute(f'select i4, array_enumerate(i4) from {table}')
    for arr, s in result:
        assert sum(arr) == s


def test_literal_cast(heavydb):

    @heavydb('bool(int32[])')
    def literal_cast(arr):
        if len(arr) > 2:
            return True
        return False

    _, result = heavydb.sql_execute(f'select literal_cast(i4) from {heavydb.table_name}')
    result = list(result)
    assert result == [(1,), (1,), (1,), (1,), (1,)]
