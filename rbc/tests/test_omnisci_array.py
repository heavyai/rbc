import pytest


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


def _test_get_array_size_ir1(omnisci):
    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    device_params = omnisci.thrift_call('get_device_parameters')
    cpu_target_triple = device_params['cpu_triple']
    cpu_target_datalayout = "e-m:e-i64:64-f80:128-n8:16:32:64-S128"

    # The following are codelets from clang --emit-llvm output with
    # attributes removed and change of function names:
    Array_getSize_i32_ir = '''\
define dso_local i64 @Array_getSize_i32(%struct.Array*) align 2 {
  %2 = alloca %struct.Array*, align 8
  store %struct.Array* %0, %struct.Array** %2, align 8
  %3 = load %struct.Array*, %struct.Array** %2, align 8
  %4 = getelementptr inbounds %struct.Array, %struct.Array* %3, i32 0, i32 1
  %5 = load i64, i64* %4, align 8
  ret i64 %5
}
'''

    array_sz_i32_ir = '''\
define dso_local i32 @array_sz_int32(%struct.Array* byval align 8) {
  %2 = call i64 @Array_getSize_i32(%struct.Array* %0)
  %3 = trunc i64 %2 to i32
  ret i32 %3
}'''

    ast_signatures = "array_sz_int32 'int32_t(Array<int32_t>)'"

    device_ir_map = dict()
    device_ir_map['cpu'] = '''\
target datalayout = "{cpu_target_datalayout}"
target triple = "{cpu_target_triple}"

%struct.Array = type {{ i32*, i64, i8 }}

{Array_getSize_i32_ir}

{array_sz_i32_ir}
'''.format(**locals())

    omnisci.thrift_call('register_runtime_udf',
                        omnisci.session_id,
                        ast_signatures, device_ir_map)

    desrc, result = omnisci.sql_execute(
        'select i4, array_sz_int32(i4) from {omnisci.table_name}'
        .format(**locals()))
    for a, sz in result:
        assert len(a) == sz


def test_len_i32(omnisci):
    omnisci.reset()

    @omnisci('int64(int32[])')
    def array_sz_int32(x):
        return len(x)
    desrc, result = omnisci.sql_execute(
        'select i4, array_sz_int32(i4) from {omnisci.table_name}'
        .format(**locals()))
    for a, sz in result:
        assert len(a) == sz


def test_len_f64(omnisci):
    omnisci.reset()

    @omnisci('int64(float64[])')
    def array_sz_double(x):
        return len(x)

    desrc, result = omnisci.sql_execute(
        'select f8, array_sz_double(f8) from {omnisci.table_name}'
        .format(**locals()))
    for a, sz in result:
        assert len(a) == sz


def test_getitem_i8(omnisci):
    omnisci.reset()

    @omnisci('int8(int8[], int32)')
    def array_getitem_int8(x, i):
        return x[i]

    query = ('select i1, array_getitem_int8(i1, 2) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_i32(omnisci):
    omnisci.reset()

    @omnisci('int32(int32[], int32)')
    def array_getitem_int32(x, i):
        return x[i]

    query = ('select i4, array_getitem_int32(i4, 2) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_i64(omnisci):
    omnisci.reset()

    @omnisci('int64(int64[], int64)')
    def array_getitem_int64(x, i):
        return x[i]

    query = ('select i8, array_getitem_int64(i8, 2) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_getitem_float(omnisci):
    omnisci.reset()

    @omnisci('double(double[], int32)')
    def array_getitem_double(x, i):
        return x[i]

    query = ('select f8, array_getitem_double(f8, 2) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, item in result:
        assert a[2] == item
        assert type(a[2]) == type(item)

    @omnisci('float(float[], int64)')
    def array_getitem_float(x, i):
        return x[i]

    query = ('select f4, array_getitem_float(f4, 2) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, item in result:
        assert a[2] == item
        assert type(a[2]) == type(item)


@pytest.mark.skipif(available_version[:2] == (5, 1),
                    reason="skip due to a bug in omniscidb 5.1 (got %s)" % (
                        available_version,))
def test_getitem_bool(omnisci):
    omnisci.reset()

    @omnisci('bool(bool[], int64)')
    def array_getitem_bool(x, i):
        return x[i]

    query = ('select b, array_getitem_bool(b, 2) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, item in result:
        assert a[2] == item


def test_sum(omnisci):
    omnisci.reset()

    @omnisci('int32(int32[])')
    def array_sum_int32(x):
        r = 0
        n = len(x)
        for i in range(n):
            r = r + x[i]
        return r

    query = ('select i4, array_sum_int32(i4) from {omnisci.table_name}'
             .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for a, s in result:
        assert sum(a) == s


@pytest.mark.skipif(available_version[:2] == (5, 1),
                    reason="skip due to a bug in omniscidb 5.1 (got %s)" % (
                        available_version,))
def test_even_sum(omnisci):
    omnisci.reset()

    @omnisci('int32(bool[], int32[])')
    def array_even_sum_int32(b, x):
        r = 0
        n = len(x)
        for i in range(n):
            if b[i]:
                r = r + x[i]
        return r

    query = (
        'select b, i4, array_even_sum_int32(b, i4) from {omnisci.table_name}'
        .format(**locals()))
    desrc, result = omnisci.sql_execute(query)
    for b, i4, s in result:
        assert sum([i_ for b_, i_ in zip(b, i4) if b_]) == s


def test_array_setitem(omnisci):
    if omnisci.has_cuda:
        pytest.skip(
            'test_array_setitem: crashes CUDA enabled omniscidb server'
            ' [rbc issue 72]')
    omnisci.reset()

    @omnisci('double(double[], int32)')
    def array_setitem_sum(b, c):
        n = len(b)
        s = 0
        for i in range(n):
            b[i] = b[i] * c  # changes the value inplace
            s += b[i]
            b[i] = b[i] / c
        return s

    query = (
        'select f8, array_setitem_sum(f8, 4) from {omnisci.table_name}'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    for f8, s in result:
        assert sum(f8) * 4 == s


def test_array_constructor_noreturn(omnisci):
    if omnisci.has_cuda:
        pytest.skip(
            'crashes CUDA enabled omniscidb server [issue 94]')

    omnisci.reset()

    from rbc.omnisci_backend import Array
    from numba import types

    @omnisci('float64(int32)')
    def array_noreturn(size):
        a = Array(size, types.float64)
        b = Array(size, types.float64)
        c = Array(size, types.float64)
        for i in range(size):
            a[i] = b[i] = c[i] = i + 3.0
        s = 0.0
        for i in range(size):
            s += a[i] + b[i] + c[i] - a[i] * b[i]
        return s

    query = (
        'select array_noreturn(10)'
        .format(**locals())
    )

    _, result = omnisci.sql_execute(query)
    r = list(result)[0]
    assert (r == (-420.0,))


def test_array_constructor_return(omnisci):
    omnisci.reset()

    from rbc.omnisci_backend import Array
    from numba import types
    from rbc.irtools import printf

    @omnisci('float64[](int32)')
    def array_return(size):
        printf("entering array_return(%i)\n", size)
        a = Array(size, types.float64)
        b = Array(size, types.float64)
        for i in range(size):
            a[i] = float(i)
            b[i] = float(size - i - 1)
        if size % 2:
            c = a
        else:
            c = b
        printf("returning array with length %i\n", len(c))
        return c

    query = (
        'select array_return(9), array_return(10)'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    r = list(result)[0]
    assert r == (list(map(float, range(9))),
                 list(map(float, reversed(range(10)))))


def test_array_constructor_len(omnisci):
    omnisci.reset()

    from rbc.omnisci_backend import Array
    from numba import types

    @omnisci('int64(int32)')
    def array_len(size):
        a = Array(size, types.float64)
        return len(a)

    query = (
        'select array_len(30)'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    assert list(result)[0] == (30,)


def test_array_constructor_getitem(omnisci):
    if omnisci.has_cuda:
        pytest.skip(
            'test_array_constructor_getitem: crashes CUDA enabled omniscidb'
            ' server [rbc issue 72]')
    omnisci.reset()

    from rbc.omnisci_backend import Array
    import numpy as np

    @omnisci('double(int32, int32)')
    def array_ptr(size, pos):
        a = Array(size, np.double)
        for i in range(size):
            a[i] = i + 0.0
        return a[pos]

    query = (
        'select array_ptr(5, 3)'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    assert list(result)[0] == (3.0,)


def test_array_constructor_is_null(omnisci):
    omnisci.reset()

    from rbc.omnisci_backend import Array

    @omnisci('int8(int64)')
    def array_is_null(size):
        a = Array(size, 'double')
        return a.is_null()

    query = (
        'select array_is_null(3);'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    assert list(result)[0] == (0,)
