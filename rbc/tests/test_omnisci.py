import os
from rbc import errors
import numpy as np
import pytest


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
# Throw an error on Travis CI if the server is not available
if "TRAVIS" in os.environ and not available_version:
    pytest.exit(msg=reason, returncode=1)
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


def test_get_client_config(tmpdir):
    d = tmpdir.mkdir("omnisci")
    fh = d.join("client.conf")
    fh.write("""
    [user]
name  =  foo
password = secret

[server]
port: 1234
host: example.com

[rbc]
debug: False
use_host_target: False
# server: Server [NOT IMPL]
# target_info: TargetInfo
""")
    conf_file = os.path.join(fh.dirname, fh.basename)

    old_conf = os.environ.get('OMNISCI_CLIENT_CONF')
    os.environ['OMNISCI_CLIENT_CONF'] = conf_file

    try:
        conf = rbc_omnisci.get_client_config()
        assert conf['user'] == 'foo'
        assert conf['password'] == 'secret'
        assert conf['port'] == 1234
        assert conf['host'] == 'example.com'
        assert conf['dbname'] == 'omnisci'
        assert conf['debug'] == bool(0)
        conf = rbc_omnisci.get_client_config(dbname='test')
        assert conf['dbname'] == 'test'
    finally:
        if old_conf is None:
            del os.environ['OMNISCI_CLIENT_CONF']
        else:
            os.environ['OMNISCI_CLIENT_CONF'] = old_conf


@pytest.fixture(scope='module')
def nb_version():
    from rbc.utils import get_version
    return get_version('numba')


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = 'rbc_test_omnisci'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN']
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
            return ("'true'" if row % 2 == 0 else "'false'")
        return row

    rows = 5
    for i in range(rows):
        table_row = ', '.join(str(row_value(i, j, n))
                              for j, n in enumerate(colnames))
        m.sql_execute(
            'INSERT INTO {table_name} VALUES ({table_row})'.format(**locals()))
    m.table_name = table_name
    yield m
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))


def test_redefine(omnisci):
    omnisci.reset()

    @omnisci('i32(i32)')
    def incr(x):
        return x + 1

    desrc, result = omnisci.sql_execute(
        'select i4, incr(i4) from {omnisci.table_name}'.format(**locals()))
    for x, x1 in result:
        assert x1 == x + 1

    # Re-defining triggers a warning message when in debug mode
    @omnisci('i32(i32)')  # noqa: F811
    def incr(x):  # noqa: F811
        return x + 2

    desrc, result = omnisci.sql_execute(
        'select i4, incr(i4) from {omnisci.table_name}'.format(**locals()))
    for x, x1 in result:
        assert x1 == x + 2


def test_forbidden_define(omnisci):
    if omnisci.version > (5, 1):
        pytest.skip(
            f'forbidden defines not required for OmnisciDB {omnisci.version}')

    omnisci.reset()

    msg = "Attempt to define function with name `{name}`"

    @omnisci('double(double)')
    def sinh(x):
        return np.sinh(x)

    with pytest.raises(errors.ForbiddenNameError) as excinfo:
        omnisci.register()
    assert msg.format(name='sinh') in str(excinfo.value)

    omnisci.reset()

    @omnisci('double(double)')
    def trunc(x):
        return np.trunc(x)

    with pytest.raises(errors.ForbiddenNameError) as excinfo:
        omnisci.register()
    assert msg.format(name='trunc') in str(excinfo.value)

    omnisci.reset()


def test_single_argument_overloading(omnisci):
    omnisci.reset()

    @omnisci(
        'f64(f64)',
        'i64(i64)',
        'i32(i32)',
        'f32(f32)',
        # 'i32(f32)',
    )
    def mydecr(x):
        return x - 1
    desrc, result = omnisci.sql_execute(
        'select f4, mydecr(f4) from {omnisci.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))
    desrc, result = omnisci.sql_execute(
        'select f8, mydecr(f8) from {omnisci.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))
    desrc, result = omnisci.sql_execute(
        'select i4, mydecr(i4) from {omnisci.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))


def test_numpy_forbidden_ufunc(omnisci, nb_version):
    omnisci.reset()

    if omnisci.version >= (5, 5) and nb_version >= (0, 52):
        pytest.skip(
            f'forbidden ufunc not required for OmniSciDB {omnisci.version} '
            f'and Numba {nb_version}')

    if not omnisci.has_cuda:
        pytest.skip('forbidden ufunc not required for CUDA-disabled OmniSciDB')

    if omnisci.version >= (5, 5):
        # Requires: https://github.com/omnisci/omniscidb-internal/pull/4955
        pytest.skip('forbidden ufunc not required if CPU ufuncs work [omniscidb-interal PR 4955]')

    msg = "Attempt to use function with name `{ufunc}`"

    @omnisci('double(double)')
    def arcsin(x):
        return np.arcsin(x)

    with pytest.raises(errors.ForbiddenIntrinsicError) as excinfo:
        omnisci.register()
    assert msg.format(ufunc='asin') in str(excinfo.value)

    omnisci.reset()

    @omnisci('float32(float32, float32)')
    def logaddexp(x, y):
        return np.logaddexp(x, y)

    with pytest.raises(errors.ForbiddenIntrinsicError) as excinfo:
        omnisci.register()
    assert msg.format(ufunc='log1pf') in str(excinfo.value)

    omnisci.reset()


def test_thrift_api_doc(omnisci):
    omnisci.reset()

    @omnisci('double(int, double)',
             'float(int, float)',
             'int(int, int)')
    def foo(i, v):
        return v * i + 55

    descr, result = omnisci.sql_execute(
        'select f8, foo(i4, f8) from {omnisci.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for i, (x, x1) in enumerate(result):
        assert x1 == x * i + 55
        assert isinstance(x1, type(x))


def test_manual_ir(omnisci):
    omnisci.reset()
    descr, result = omnisci.sql_execute(
        'SELECT * FROM {omnisci.table_name}'.format(**locals()))
    result = list(result)
    assert result == [(0.0, 0.0, 0, 0, 0, 0, 1), (1.0, 1.0, 1, 1, 1, 1, 0),
                      (2.0, 2.0, 2, 2, 2, 2, 1), (3.0, 3.0, 3, 3, 3, 3, 0),
                      (4.0, 4.0, 4, 4, 4, 4, 1)]
    device_params = omnisci.thrift_call('get_device_parameters',
                                        omnisci.session_id)
    cpu_target_triple = device_params['cpu_triple']
    cpu_target_datalayout = "e-m:e-i64:64-f80:128-n8:16:32:64-S128"
    gpu_target_triple = device_params.get('gpu_triple')
    gpu_target_datalayout = ("e-p:64:64:64-i1:8:8-i8:8:8-"
                             "i16:16:16-i32:32:32-i64:64:64-"
                             "f32:32:32-f64:64:64-v16:16:16-"
                             "v32:32:32-v64:64:64-v128:128:128-n16:32:64")

    foo_ir = '''\
define i32 @foobar(i32 %.1, i32 %.2) {
entry:
  %.18.i = mul i32 %.2, %.1
  %.33.i = add i32 %.18.i, 55
  ret i32 %.33.i
}
'''
    ast_signatures = "foobar 'int32(int32, int32)'"
    device_ir_map = dict()
    device_ir_map['cpu'] = '''
target datalayout = "{cpu_target_datalayout}"
target triple = "{cpu_target_triple}"
{foo_ir}
'''.format(**locals())

    if gpu_target_triple is not None:
        device_ir_map['gpu'] = '''
target datalayout = "{gpu_target_datalayout}"
target triple = "{gpu_target_triple}"
{foo_ir}
'''.format(**locals())

    omnisci.thrift_call('register_runtime_udf', omnisci.session_id,
                        ast_signatures, device_ir_map)
    omnisci._last_ir_map = {}  # hack
    descr, result = omnisci.sql_execute(
        'SELECT i4, foobar(i4, i4) FROM {omnisci.table_name}'
        .format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, r in result:
        assert r == x * x + 55


def test_ir_parse_error(omnisci):
    device_params = omnisci.thrift_call('get_device_parameters',
                                        omnisci.session_id)
    foo_ir = '''\
define i32 @foobar(i32 %.1, i32 %.2) {
entry:
  %.18.i = mul i32 %.2, %.1
  %.33.i = add i32 %.18.i, 55
  ret i32 %.33.i

'''
    ast_signatures = "foobar 'int32(int32, int32)'"
    device_ir_map = dict()
    device_ir_map['cpu'] = foo_ir

    gpu_target_triple = device_params.get('gpu_triple')
    if gpu_target_triple is not None:
        device_ir_map['gpu_triple'] = foo_ir

    with pytest.raises(Exception, match=r".*LLVM IR ParseError:"):
        omnisci.thrift_call('register_runtime_udf', omnisci.session_id,
                            ast_signatures, device_ir_map)


def test_ir_query_error(omnisci):
    pytest.skip("requires omniscidb-internal catching undefined symbols")

    device_params = omnisci.thrift_call('get_device_parameters',
                                        omnisci.session_id)
    gpu_target_triple = device_params.get('gpu_triple')
    foo_ir = '''\
define i32 @foobarrr(i32 %.1, i32 %.2) {
entry:
  %.18.i = mul i32 %.2, %.1
  %.33.i = add i32 %.18.i, 55
  ret i32 %.33.i
}
'''
    ast_signatures = "foobar 'int32(int32, int32)'"
    device_ir_map = dict()
    device_ir_map['cpu'] = foo_ir
    if gpu_target_triple is not None:
        device_ir_map['gpu'] = foo_ir

    omnisci.thrift_call('register_runtime_udf', omnisci.session_id,
                        ast_signatures, device_ir_map)
    try:
        omnisci.sql_execute(
            'SELECT i4, foobar(i4, i4) FROM {omnisci.table_name}'
            .format(**locals()))
    except errors.OmnisciServerError as msg:
        assert "use of undefined value '@foobar'" in str(msg)


def test_multiple_implementation(omnisci):
    omnisci.reset()

    @omnisci('int(f64)', 'int(i64)')  # noqa: F811
    def bits(x):
        return 64

    @omnisci('int(f32)', 'int(i32)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 32

    @omnisci('int(i16)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 16

    @omnisci('int(i8)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 8

    descr, result = omnisci.sql_execute(
        'select bits(i1), bits(i2), bits(i4), bits(f4), bits(i8), bits(f8)'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    result = list(result)
    assert len(result) == 1
    assert result[0] == (8, 16, 32, 32, 64, 64)


def test_loadtime_udf(omnisci):
    # This test requires that omnisci server is started with `--udf
    # sample_udf.cpp` option where the .cpp file defines `udf_diff`
    try:
        desrc, result = omnisci.sql_execute(
            'select i4, udf_diff2(i4, i4) from {omnisci.table_name}'
            .format(**locals()))
    except Exception as msg:
        assert 'No match found for function signature udf_diff' in str(msg)
        return
    result = list(result)
    for i_, (i, d) in enumerate(result):
        assert i_ == i
        assert d == 0


def test_f32(omnisci):
    """If UDF name ends with an underscore, expect strange behaviour. For
    instance, defining

      @omnisci('f32(f32)', 'f32(f64)')
      def f32_(x): return x+4.5

    the query `select f32_(0.0E0))` fails but not when defining

      @omnisci('f32(f64)', 'f32(f32)')
      def f32_(x): return x+4.5

    (notice the order of signatures in omnisci decorator argument).
    """

    @omnisci('f32(f32)', 'f32(f64)')  # noqa: F811
    def f_32(x): return x+4.5
    descr, result = omnisci.sql_execute(
        'select f_32(0.0E0) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5,)


def test_castop(omnisci):
    @omnisci('i16(i16)')  # noqa: F811
    def i32(x): return x+2  # noqa: F811

    @omnisci('i32(i32)')  # noqa: F811
    def i32(x): return x+4  # noqa: F811

    descr, result = omnisci.sql_execute(
        'select i32(cast(1 as int))'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (5,)


def test_casting(omnisci):
    """
    Define UDFs:
      i8(<tinyint>), i16(<smallint>), i32(<int>), i64(<bigint>)
      f32(<float>), f64(<double>)

    The following table defines the behavior of applying these UDFs to
    values with different types:

             | Functions applied to <itype value>
    itype    | i8   | i16  | i32  | i64  | f32  | f64  |
    ---------+------+------+------+------+------+------+
    tinyint  | OK   | OK   | OK   | OK   | FAIL | FAIL |
    smallint | FAIL | OK   | OK   | OK   | FAIL | FAIL |
    int      | FAIL | FAIL | OK   | OK   | FAIL | FAIL |
    bigint   | FAIL | FAIL | FAIL | OK   | FAIL | FAIL |
    float    | FAIL | FAIL | FAIL | FAIL | OK   | OK   |
    double   | FAIL | FAIL | FAIL | FAIL | FAIL | OK   |
    """
    omnisci.reset()

    @omnisci('i8(i8)')    # noqa: F811
    def i8(x): return x+1

    @omnisci('i16(i16)')  # noqa: F811
    def i16(x): return x+2

    @omnisci('i32(i32)')  # noqa: F811
    def i32(x): return x+4

    @omnisci('i64(i64)')  # noqa: F811
    def i64(x): return x+8

    @omnisci('f32(f32)')  # noqa: F811
    def f32(x): return x+4.5

    # cannot create a 8-bit and 16-bit int literals in sql, so, using
    # the following helper functions:
    @omnisci('i8(i8)', 'i8(i16)', 'i8(i32)')    # noqa: F811
    def i_8(x): return x+1

    @omnisci('i16(i16)', 'i16(i32)')  # noqa: F811
    def i_16(x): return x+2

    # cannot create a 32-bit float literal in sql, so, using a helper
    # function for that:
    @omnisci('f32(f32)', 'f32(f64)')  # noqa: F811
    def f_32(x): return x+4.5

    @omnisci('f64(f64)')  # noqa: F811
    def f64(x): return x+8.5

    @omnisci('i8(i8)')
    def ifoo(x): return x + 1

    @omnisci('i16(i16)')  # noqa: F811
    def ifoo(x): return x + 2  # noqa: F811

    @omnisci('i32(i32)')  # noqa: F811
    def ifoo(x): return x + 4  # noqa: F811

    @omnisci('i64(i64)')  # noqa: F811
    def ifoo(x): return x + 8  # noqa: F811

    @omnisci('f32(f32)')
    def ffoo(x): return x + 4.5  # noqa: F811

    @omnisci('f64(f64)')  # noqa: F811
    def ffoo(x): return x + 8.5  # noqa: F811

    descr, result = omnisci.sql_execute(
        'select i_8(0),i_16(0),i32(0),i64(0) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1, 2, 4, 8)

    descr, result = omnisci.sql_execute(
        'select i_8(i1),i16(i2),i32(i4),i64(i8)'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1, 2, 4, 8)

    descr, result = omnisci.sql_execute(
        'select ifoo(i_8(0)),ifoo(i_16(0)),ifoo(i32(0)),ifoo(i64(0))'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1+1, 2+2, 4+4, 8+8)

    descr, result = omnisci.sql_execute(
        'select ifoo(i1),ifoo(i2),ifoo(i4),ifoo(i8)'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1, 2, 4, 8)

    descr, result = omnisci.sql_execute(
        'select i64(i_8(0)), i64(i_16(0)),i64(i32(0)),i64(i64(0))'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (9, 10, 12, 16)

    descr, result = omnisci.sql_execute(
        'select i64(i1), i64(i2),i64(i4),i64(i8)'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (8, 8, 8, 8)

    descr, result = omnisci.sql_execute(
        'select i32(i_8(0)), i32(i_16(0)),i32(i32(0))'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (5, 6, 8)

    descr, result = omnisci.sql_execute(
        'select i32(i1), i32(i2),i32(i4)'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (4, 4, 4)

    descr, result = omnisci.sql_execute(
        'select i16(i_8(0)), i16(i_16(0)) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (3, 4)

    descr, result = omnisci.sql_execute(
        'select i16(i1), i16(i2) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (2, 2)

    descr, result = omnisci.sql_execute(
        'select i8(i_8(0)) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (2,)

    descr, result = omnisci.sql_execute(
        'select i8(i1) from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (1,)

    descr, result = omnisci.sql_execute(
        'select f_32(0.0E0), f64(0.0E0) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5, 8.5)

    descr, result = omnisci.sql_execute(
        'select f_32(f4), f64(f8) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5, 8.5)

    descr, result = omnisci.sql_execute(
        'select f64(f_32(0.0E0)), f64(f64(0.0E0))'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (13.0, 17.0)

    descr, result = omnisci.sql_execute(
        'select f64(f4), f64(f8)'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (8.5, 8.5)

    descr, result = omnisci.sql_execute(
        'select f32(f_32(0.0E0)) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (9.0,)

    descr, result = omnisci.sql_execute(
        'select f32(f4) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5,)

    descr, result = omnisci.sql_execute(
        'select ffoo(f_32(0.0E0)), ffoo(f64(0.0E0))'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (9.0, 17.0)

    descr, result = omnisci.sql_execute(
        'select ffoo(f4), ffoo(f8)'
        ' from {omnisci.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (4.5, 8.5)

    for v, f, t in [
            ('i64(0)', r'i32', r'BIGINT'),
            ('i64(0)', r'i16', r'BIGINT'),
            ('i64(0)', r'i8', r'BIGINT'),
            ('i8', r'i32', r'BIGINT'),
            ('i8', r'i16', r'BIGINT'),
            ('i8', r'i8', r'BIGINT'),
            ('i32(0)', r'i16', r'INTEGER'),
            ('i32(0)', r'i8', r'INTEGER'),
            ('i4', r'i16', r'INTEGER'),
            ('i4', r'i8', r'INTEGER'),
            ('i_16(0)', r'i8', r'SMALLINT'),
            ('i2', r'i8', r'SMALLINT'),
            ('f64(0)', r'f32', r'DOUBLE'),
            ('f8', r'f32', r'DOUBLE'),
    ]:
        q = ('select '+f+'('+v+') from {omnisci.table_name} limit 1'
             .format(**locals()))
        match = (r".*(Function "+f+r"\("+t+r"\) not supported"
                 r"|Could not bind "+f+r"\("+t+r"\))")
        with pytest.raises(Exception, match=match):
            descr, result = omnisci.sql_execute(q)
            print('query: ', q)
            print('expected: ', match)

    for f in [r'f64', r'f32']:
        for at, av, r in [
                (r'BIGINT', 'i64(0)', (16.5,)),
                (r'INTEGER', 'i32(0)', (12.5,)),
                (r'SMALLINT', 'i_16(0)', (10.5,)),
                (r'TINYINT', 'i_8(0)', (9.5,)),
                (r'BIGINT', 'i8', (8.5,)),
                (r'INTEGER', 'i4', (8.5,)),
                (r'SMALLINT', 'i2', (8.5,)),
                (r'TINYINT', 'i1', (8.5,)),
        ]:
            if f == r'f64':  # temporary: allow integers as double arguments
                descr, result = omnisci.sql_execute(
                    'select '+f+'('+av+') from {omnisci.table_name} limit 1'
                    .format(**locals()))
                assert list(result)[0] == r
                continue
            with pytest.raises(
                    Exception,
                    match=(r".*(Function "+f+r"\("+at+r"\) not supported"
                           r"|Could not bind "+f+r"\("+at+r"\))")):
                descr, result = omnisci.sql_execute(
                    'select '+f+'('+av+') from {omnisci.table_name} limit 1'
                    .format(**locals()))

    for f in [r'i64', r'i32', r'i16', r'i8']:
        for at, av in [
                (r'DOUBLE', 'f64(0E0)'),
                (r'FLOAT', 'f_32(0E0)'),
                (r'DOUBLE', 'f8'),
                (r'FLOAT', 'f4'),
        ]:
            with pytest.raises(
                    Exception,
                    match=(r".*(Function "+f+r"\("+at+r"\) not supported"
                           r"|Could not bind "+f+r"\("+at+r"\))")):
                descr, result = omnisci.sql_execute(
                    'select '+f+'('+av+') from {omnisci.table_name} limit 1'
                    .format(**locals()))


def test_truncate_issue(omnisci):
    omnisci.reset()

    @omnisci('int(f64)', 'int(i64)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 64

    @omnisci('int(f32)', 'int(i32)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 32

    @omnisci('int(i16)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 16

    @omnisci('int(i8)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 8

    descr, result = omnisci.sql_execute(
        'select bits(truncate(2016, 1)) from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] in [(16,), (32,)]

    descr, result = omnisci.sql_execute(
        'select bits(truncate(cast(2016.0 as smallint), 1))'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (16,)

    descr, result = omnisci.sql_execute(
        'select bits(truncate(cast(2016.0 as int), 1))'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (32,)

    descr, result = omnisci.sql_execute(
        'select bits(truncate(cast(2016.0 as bigint), 1))'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (64,)

    descr, result = omnisci.sql_execute(
        'select bits(truncate(cast(2016.0 as float), 1))'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (32,)

    descr, result = omnisci.sql_execute(
        'select bits(truncate(cast(2016.0 as double), 1))'
        ' from {omnisci.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (64,)
