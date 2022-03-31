import os
import itertools
import pytest
import numpy as np

from rbc.errors import UnsupportedError, HeavyDBServerError
from rbc.tests import heavydb_fixture, assert_equal
from rbc.typesystem import Type

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
# Throw an error on Travis CI if the server is not available
if "TRAVIS" in os.environ and not available_version:
    pytest.exit(msg=reason, returncode=1)
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.mark.parametrize("heavydb_brand", ['omnisci', 'heavyai'])
def test_get_client_config(tmpdir, heavydb_brand):
    d = tmpdir.mkdir(heavydb_brand)
    fh = d.join("client.conf")
    fh.write(f"""
    [user]
name  =  foo
password = secret

[server]
port: 1234
host: example.com
dbname: {heavydb_brand}

[rbc]
debug: False
use_host_target: False
# server: Server [NOT IMPL]
# target_info: TargetInfo
""")
    conf_file = os.path.join(fh.dirname, fh.basename)

    client_conf_env = dict(heavyai='HEAVYDB_CLIENT_CONF',
                           omnisci='OMNISCI_CLIENT_CONF')[heavydb_brand]
    old_conf = os.environ.get(client_conf_env)
    os.environ[client_conf_env] = conf_file

    try:
        conf = rbc_heavydb.get_client_config()
        assert conf['user'] == 'foo'
        assert conf['password'] == 'secret'
        assert conf['port'] == 1234
        assert conf['host'] == 'example.com'
        assert conf['dbname'] == heavydb_brand
        assert conf['debug'] == bool(0)
        conf = rbc_heavydb.get_client_config(dbname='test')
        assert conf['dbname'] == 'test'
    finally:
        if old_conf is None:
            del os.environ[client_conf_env]
        else:
            os.environ[client_conf_env] = old_conf


@pytest.fixture(scope='module')
def nb_version():
    from rbc.utils import get_version
    return get_version('numba')


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals()):
        yield o


def test_direct_call(heavydb):
    heavydb.reset()

    @heavydb('double(double)')
    def farhenheit2celcius(f):
        return (f - 32) * 5 / 9

    assert_equal(farhenheit2celcius(40).execute(), np.float32(40 / 9))


def test_local_caller(heavydb):
    heavydb.reset()

    def func(f):
        return f

    caller = heavydb('double(double)')(func)

    msg = "Cannot create a local `Caller`"
    with pytest.raises(UnsupportedError, match=msg):
        _ = caller.local


def test_redefine(heavydb):
    heavydb.reset()

    @heavydb('i32(i32)')
    def incr(x):
        return x + 1

    desrc, result = heavydb.sql_execute(
        f'select i4, incr(i4) from {heavydb.table_name}')
    for x, x1 in result:
        assert x1 == x + 1

    # Re-defining triggers a warning message when in debug mode
    @heavydb('i32(i32)')  # noqa: F811
    def incr(x):  # noqa: F811
        return x + 2

    desrc, result = heavydb.sql_execute(
        f'select i4, incr(i4) from {heavydb.table_name}')
    for x, x1 in result:
        assert x1 == x + 2


def test_single_argument_overloading(heavydb):
    heavydb.reset()

    @heavydb(
        'f64(f64)',
        'i64(i64)',
        'i32(i32)',
        'f32(f32)',
        # 'i32(f32)',
    )
    def mydecr(x):
        return x - 1
    desrc, result = heavydb.sql_execute(
        'select f4, mydecr(f4) from {heavydb.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))
    desrc, result = heavydb.sql_execute(
        'select f8, mydecr(f8) from {heavydb.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))
    desrc, result = heavydb.sql_execute(
        'select i4, mydecr(i4) from {heavydb.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for x, x1 in result:
        assert x1 == x - 1
        assert isinstance(x1, type(x))


def test_thrift_api_doc(heavydb):
    heavydb.reset()

    @heavydb('double(int, double)',
             'float(int, float)',
             'int(int, int)')
    def foo(i, v):
        return v * i + 55

    descr, result = heavydb.sql_execute(
        'select f8, foo(i4, f8) from {heavydb.table_name}'.format(**locals()))
    result = list(result)
    assert len(result) > 0
    for i, (x, x1) in enumerate(result):
        assert x1 == x * i + 55
        assert isinstance(x1, type(x))


def test_multiple_implementation(heavydb):
    heavydb.reset()

    @heavydb('int(f64)', 'int(i64)')  # noqa: F811
    def bits(x):
        return 64

    @heavydb('int(f32)', 'int(i32)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 32

    @heavydb('int(i16)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 16

    @heavydb('int(i8)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 8

    descr, result = heavydb.sql_execute(
        'select bits(i1), bits(i2), bits(i4), bits(f4), bits(i8), bits(f8)'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    result = list(result)
    assert len(result) == 1
    assert result[0] == (8, 16, 32, 32, 64, 64)


def test_loadtime_udf(heavydb):
    # This test requires that heavydb server is started with `--udf
    # sample_udf.cpp` option where the .cpp file defines `udf_diff`
    try:
        desrc, result = heavydb.sql_execute(
            'select i4, udf_diff2(i4, i4) from {heavydb.table_name}'
            .format(**locals()))
    except Exception as msg:
        assert "Undefined function call 'udf_diff2'" in str(msg)
        return
    result = list(result)
    for i_, (i, d) in enumerate(result):
        assert i_ == i
        assert d == 0


def test_f32(heavydb):
    """If UDF name ends with an underscore, expect strange behaviour. For
    instance, defining

      @heavydb('f32(f32)', 'f32(f64)')
      def f32_(x): return x+4.5

    the query `select f32_(0.0E0))` fails but not when defining

      @heavydb('f32(f64)', 'f32(f32)')
      def f32_(x): return x+4.5

    (notice the order of signatures in heavydb decorator argument).
    """

    @heavydb('f32(f32)', 'f32(f64)')  # noqa: F811
    def f_32(x): return x+4.5
    descr, result = heavydb.sql_execute(
        'select f_32(0.0E0) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5,)


def test_castop(heavydb):
    @heavydb('i16(i16)')  # noqa: F811
    def i32(x): return x+2  # noqa: F811

    @heavydb('i32(i32)')  # noqa: F811
    def i32(x): return x+4  # noqa: F811

    descr, result = heavydb.sql_execute(
        'select i32(cast(1 as int))'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (5,)


def test_binding(heavydb):
    heavydb.reset()
    full = os.environ.get('RBC_TESTS_FULL', 'FALSE').lower() in ['1', 'true', 'on']

    if full:
        argument_types = ['i8', 'i16', 'i32', 'i64', 'f32', 'f64']
        literals = ['0', '0.0', 'cast(0 as tinyint)', 'cast(0 as smallint)', 'cast(0 as int)',
                    'cast(0 as bigint)', 'cast(0 as float)', 'cast(0 as double)']
        literals_types = ['i32', 'f32'] + argument_types
        column_vars = ['cast(i8 as tinyint)', 'cast(i8 as smallint)', 'cast(i8 as int)',
                       'cast(i8 as bigint)', 'cast(i8 as float)', 'cast(i8 as double)']
        column_vars_types = argument_types
    else:
        # skip smallint and int
        argument_types = ['i8', 'i64', 'f32', 'f64']
        literals = ['0', '0.0', 'cast(0 as tinyint)', 'cast(0 as bigint)',
                    'cast(0 as float)', 'cast(0 as double)']
        literals_types = ['i32', 'f32'] + argument_types
        column_vars = ['cast(i8 as tinyint)', 'cast(i8 as bigint)',
                       'cast(i8 as float)', 'cast(i8 as double)']
        column_vars_types = argument_types

    if available_version[:2] >= (5, 9):
        heavydb.require_version((5, 9), 'Requires heavydb-internal PR 6003')

        def get_result(overload_types, input_type, is_literal):
            overload_types_ = overload_types[::-1 if is_literal else 1]
            for overload_type in overload_types_:
                if input_type[0] == overload_type[0]:
                    if int(input_type[1:]) <= int(overload_type[1:]):
                        return overload_type
            if input_type[0] == 'i':
                for overload_type in reversed(overload_types):
                    if overload_type[0] == 'f':
                        return overload_type
            return 'NO BINDING FOUND'

    elif available_version[:2] == (5, 8):

        def get_result(overload_types, input_type, is_literal):
            overload_types_ = overload_types[::-1 if is_literal else 1]
            for overload_type in overload_types_:
                if input_type[0] == overload_type[0]:
                    if int(input_type[1:]) <= int(overload_type[1:]):
                        return overload_type
            if input_type[0] == 'i':
                for overload_type in overload_types_:
                    if overload_type[0] == 'f' and int(input_type[1:]) <= int(overload_type[1:]):
                        return overload_type
            return 'NO BINDING FOUND'

    else:
        # typeless literal expressions not support as arguments
        literals = literals[2:]
        literals_types = literals_types[2:]

        def get_result(overload_types, input_type, is_literal):
            for overload_type in overload_types:
                if input_type[0] == overload_type[0]:
                    if int(input_type[1:]) <= int(overload_type[1:]):
                        return overload_type
                elif overload_type == 'f64':
                    return overload_type
            return 'NO BINDING FOUND'

    assert len(literals) == len(literals_types)
    assert len(column_vars) == len(column_vars_types)

    heavydb.reset()
    functions = []
    for r in range(1, len(argument_types) + 1):
        if not full and r not in [1, 2, len(argument_types)]:
            continue
        for p in itertools.combinations(range(len(argument_types)), r=r):
            fname = 'rt_binding_' + '_'.join(map(argument_types.__getitem__, p))
            for i in p:
                foo = eval(f'lambda x: {i}')
                foo.__name__ = fname
                heavydb(f'i8({argument_types[i]})')(foo)
            functions.append(fname)
    heavydb.register()

    for fname in functions:
        for input, input_type in zip(literals, literals_types):
            try:
                descr, result = heavydb.sql_execute(f'select {fname}({input})')
                result = list(result)
                result = argument_types[result[0][0]]
            except Exception:
                result = 'NO BINDING FOUND'
            expected = get_result(fname[len('rt_binding_'):].split('_'), input_type, True)
            assert result == expected, (fname, input, input_type, result, expected)

        for input, input_type in zip(column_vars, column_vars_types):
            try:
                descr, result = heavydb.sql_execute(
                    f'select {fname}({input}) from {heavydb.table_name} limit 1')
                result = list(result)
                result = argument_types[result[0][0]]
            except Exception:
                result = 'NO BINDING FOUND'
            expected = get_result(fname[len('rt_binding_'):].split('_'), input_type, False)
            assert result == expected, (fname, input, input_type, result, expected)


def test_casting(heavydb):
    """Define UDFs:
      i8(<tinyint>), i16(<smallint>), i32(<int>), i64(<bigint>)
      f32(<float>), f64(<double>)

    The following table defines the behavior of applying these UDFs to
    values with different types:

    OmnisciDB version 5.9+
    ----------------------
             | Functions applied to <itype value>
    itype    | i8   | i16  | i32  | i64  | f32  | f64  |
    ---------+------+------+------+------+------+------+
    tinyint  | OK   | OK   | OK   | OK   | OK   | OK   |
    smallint | FAIL | OK   | OK   | OK   | OK   | OK   |
    int      | FAIL | FAIL | OK   | OK   | OK   | OK   |
    bigint   | FAIL | FAIL | FAIL | OK   | OK   | OK   |
    float    | FAIL | FAIL | FAIL | FAIL | OK   | OK   |
    double   | FAIL | FAIL | FAIL | FAIL | FAIL | OK   |

    OmnisciDB version 5.8
    ----------------------
             | Functions applied to <itype value>
    itype    | i8   | i16  | i32  | i64  | f32  | f64  |
    ---------+------+------+------+------+------+------+
    tinyint  | OK   | OK   | OK   | OK   | OK   | OK   |
    smallint | FAIL | OK   | OK   | OK   | OK   | OK   |
    int      | FAIL | FAIL | OK   | OK   | OK   | OK   |
    bigint   | FAIL | FAIL | FAIL | OK   | FAIL | OK   |
    float    | FAIL | FAIL | FAIL | FAIL | OK   | OK   |
    double   | FAIL | FAIL | FAIL | FAIL | FAIL | OK   |

    OmnisciDB version 5.7 and older
    -------------------------------
             | Functions applied to <itype value>
    itype    | i8   | i16  | i32  | i64  | f32  | f64  |
    ---------+------+------+------+------+------+------+
    tinyint  | OK   | OK   | OK   | OK   | FAIL | FAIL |
    smallint | FAIL | OK   | OK   | OK   | FAIL | FAIL |
    int      | FAIL | FAIL | OK   | OK   | FAIL | FAIL |
    bigint   | FAIL | FAIL | FAIL | OK   | FAIL | FAIL |
    float    | FAIL | FAIL | FAIL | FAIL | OK   | OK   |
    double   | FAIL | FAIL | FAIL | FAIL | FAIL | OK   |

    test_binding is superior test with respect to successful UDF
    executions but it does not check exception messages.
    """
    heavydb.reset()

    @heavydb('i8(i8)')    # noqa: F811
    def i8(x): return x+1

    @heavydb('i16(i16)')  # noqa: F811
    def i16(x): return x+2

    @heavydb('i32(i32)')  # noqa: F811
    def i32(x): return x+4

    @heavydb('i64(i64)')  # noqa: F811
    def i64(x): return x+8

    @heavydb('f32(f32)')  # noqa: F811
    def f32(x): return x+4.5

    # cannot create a 8-bit and 16-bit int literals in sql, so, using
    # the following helper functions:
    @heavydb('i8(i8)', 'i8(i16)', 'i8(i32)')    # noqa: F811
    def i_8(x): return x+1

    @heavydb('i16(i16)', 'i16(i32)')  # noqa: F811
    def i_16(x): return x+2

    # cannot create a 32-bit float literal in sql, so, using a helper
    # function for that:
    @heavydb('f32(f32)', 'f32(f64)')  # noqa: F811
    def f_32(x): return x+4.5

    @heavydb('f64(f64)')  # noqa: F811
    def f64(x): return x+8.5

    @heavydb('i8(i8)')
    def ifoo(x): return x + 1

    @heavydb('i16(i16)')  # noqa: F811
    def ifoo(x): return x + 2  # noqa: F811

    @heavydb('i32(i32)')  # noqa: F811
    def ifoo(x): return x + 4  # noqa: F811

    @heavydb('i64(i64)')  # noqa: F811
    def ifoo(x): return x + 8  # noqa: F811

    @heavydb('f32(f32)')
    def ffoo(x): return x + 4.5  # noqa: F811

    @heavydb('f64(f64)')  # noqa: F811
    def ffoo(x): return x + 8.5  # noqa: F811

    rows = []
    for itype, ivalue in [('tinyint', 'i_8(0)'),
                          ('smallint', 'i_16(0)'),
                          ('int', 'i32(0)'),
                          ('bigint', 'i64(0)'),
                          ('float', 'f_32(0.0)'),
                          ('double', 'f64(0.0)')]:
        row = [f'{itype:8}']
        cols = [f'{"itype":8}']
        for atype, func in [('tinyint', 'i8'),
                            ('smallint', 'i16'),
                            ('int', 'i32'),
                            ('bigint', 'i64'),
                            ('float', 'f32'),
                            ('double', 'f64')]:
            cols.append(f'{func:4}')
            try:
                descr, result = heavydb.sql_execute(
                    f'select {func}({ivalue}) from {heavydb.table_name} limit 1')
                status = 'OK'
            except Exception:
                status = 'FAIL'
            row.append(f'{status:4}')
        if not rows:
            rows.append(' | '.join(cols))
            rows.append('+'.join([f'{"":-^9}'] + [f'{"":-^6}'] * (len(cols)-1)))
        rows.append(' | '.join(row))

    print('\n\nSUPPORTED CASTING RULES FOR SCALAR ARGUMENTS:\n')
    print('\n'.join(rows))

    descr, result = heavydb.sql_execute(
        'select i_8(0),i_16(0),i32(0),i64(0) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1, 2, 4, 8)

    descr, result = heavydb.sql_execute(
        'select i_8(i1),i16(i2),i32(i4),i64(i8)'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1, 2, 4, 8)

    descr, result = heavydb.sql_execute(
        'select ifoo(i_8(0)),ifoo(i_16(0)),ifoo(i32(0)),ifoo(i64(0))'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1+1, 2+2, 4+4, 8+8)

    descr, result = heavydb.sql_execute(
        'select ifoo(i1),ifoo(i2),ifoo(i4),ifoo(i8)'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (1, 2, 4, 8)

    descr, result = heavydb.sql_execute(
        'select i64(i_8(0)), i64(i_16(0)),i64(i32(0)),i64(i64(0))'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (9, 10, 12, 16)

    descr, result = heavydb.sql_execute(
        'select i64(i1), i64(i2),i64(i4),i64(i8)'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (8, 8, 8, 8)

    descr, result = heavydb.sql_execute(
        'select i32(i_8(0)), i32(i_16(0)),i32(i32(0))'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (5, 6, 8)

    descr, result = heavydb.sql_execute(
        'select i32(i1), i32(i2),i32(i4)'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (4, 4, 4)

    descr, result = heavydb.sql_execute(
        'select i16(i_8(0)), i16(i_16(0)) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (3, 4)

    descr, result = heavydb.sql_execute(
        'select i16(i1), i16(i2) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (2, 2)

    descr, result = heavydb.sql_execute(
        'select i8(i_8(0)) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (2,)

    descr, result = heavydb.sql_execute(
        'select i8(i1) from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (1,)

    descr, result = heavydb.sql_execute(
        'select f_32(0.0E0), f64(0.0E0) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5, 8.5)

    descr, result = heavydb.sql_execute(
        'select f_32(f4), f64(f8) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5, 8.5)

    descr, result = heavydb.sql_execute(
        'select f64(f_32(0.0E0)), f64(f64(0.0E0))'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (13.0, 17.0)

    descr, result = heavydb.sql_execute(
        'select f64(f4), f64(f8)'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (8.5, 8.5)

    descr, result = heavydb.sql_execute(
        'select f32(f_32(0.0E0)) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (9.0,)

    descr, result = heavydb.sql_execute(
        'select f32(f4) from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (4.5,)

    descr, result = heavydb.sql_execute(
        'select ffoo(f_32(0.0E0)), ffoo(f64(0.0E0))'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
    assert list(result)[0] == (9.0, 17.0)

    descr, result = heavydb.sql_execute(
        'select ffoo(f4), ffoo(f8)'
        ' from {heavydb.table_name} limit 1'.format(**locals()))
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
        q = f'select {f}({v}) from {heavydb.table_name} limit 1'
        match = (r".*(Function "+f+r"\("+t+r"\) not supported"
                 r"|Could not bind "+f+r"\("+t+r"\))")
        with pytest.raises(Exception, match=match):
            descr, result = heavydb.sql_execute(q)
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
            if available_version[:2] >= (5, 9):
                # heavydb-internal PR 6003 changed the casting table
                if f == r'f32':
                    r = r[0] - 4,
                descr, result = heavydb.sql_execute(
                    f'select {f}({av}) from {heavydb.table_name} limit 1')
                assert list(result)[0] == r, (f, at, av, r)

            elif available_version[:2] == (5, 8):
                # heavydb-internal PR 5814 changed the casting table
                if f == r'f32' and at == r'BIGINT':
                    with pytest.raises(
                            Exception,
                            match=(r".*(Function "+f+r"\("+at+r"\) not supported"
                                   r"|Could not bind "+f+r"\("+at+r"\))")):
                        descr, result = heavydb.sql_execute(
                            f'select {f}({av}) from {heavydb.table_name} limit 1')
                else:
                    if f == r'f32':
                        r = r[0] - 4,
                    descr, result = heavydb.sql_execute(
                        f'select {f}({av}) from {heavydb.table_name} limit 1')
                    assert list(result)[0] == r, (f, at, av, r)

            elif f == r'f64':  # temporary: allow integers as double arguments
                descr, result = heavydb.sql_execute(
                    f'select {f}({av}) from {heavydb.table_name} limit 1')
                assert list(result)[0] == r

            else:
                with pytest.raises(
                        Exception,
                        match=(r".*(Function "+f+r"\("+at+r"\) not supported"
                               r"|Could not bind "+f+r"\("+at+r"\))")):
                    descr, result = heavydb.sql_execute(
                        'select '+f+'('+av+f') from {heavydb.table_name} limit 1')

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
                descr, result = heavydb.sql_execute(
                    f'select {f}({av}) from {heavydb.table_name} limit 1')


def test_truncate_issue(heavydb):
    heavydb.reset()

    @heavydb('int(f64)', 'int(i64)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 64

    @heavydb('int(f32)', 'int(i32)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 32

    @heavydb('int(i16)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 16

    @heavydb('int(i8)')  # noqa: F811
    def bits(x):  # noqa: F811
        return 8

    descr, result = heavydb.sql_execute(
        'select bits(truncate(2016, 1)) from {heavydb.table_name} limit 1'
        .format(**locals()))
    if available_version[:2] >= (5, 8):
        # heavydb-internal PR 5915 changes the casting table
        assert list(result)[0] in [(64,)]
    else:
        assert list(result)[0] in [(16,), (32,)]

    descr, result = heavydb.sql_execute(
        'select bits(truncate(cast(2016.0 as smallint), 1))'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    if available_version[:2] >= (5, 8):
        assert list(result)[0] == (64,)
    else:
        assert list(result)[0] == (16,)

    descr, result = heavydb.sql_execute(
        'select bits(truncate(cast(2016.0 as int), 1))'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    if available_version[:2] >= (5, 8):
        assert list(result)[0] == (64,)
    else:
        assert list(result)[0] == (32,)

    descr, result = heavydb.sql_execute(
        'select bits(truncate(cast(2016.0 as bigint), 1))'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (64,)

    descr, result = heavydb.sql_execute(
        'select bits(truncate(cast(2016.0 as float), 1))'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    if available_version[:2] >= (5, 8):
        assert list(result)[0] == (64,)
    else:
        assert list(result)[0] == (32,)

    descr, result = heavydb.sql_execute(
        'select bits(truncate(cast(2016.0 as double), 1))'
        ' from {heavydb.table_name} limit 1'
        .format(**locals()))
    assert list(result)[0] == (64,)


def test_unregistering(heavydb):
    heavydb.reset()

    @heavydb('i32(i32)')
    def fahrenheit2celsius(f):
        return (f - 32) * 5 / 9

    _, result = heavydb.sql_execute('select fahrenheit2celsius(40)')
    assert list(result)[0] == (4,)

    heavydb.unregister()

    msg = "Undefined function call"
    with pytest.raises(HeavyDBServerError, match=msg):
        heavydb.sql_execute('select fahrenheit2celsius(40)')


def test_format_type(heavydb):
    def test(s, caller=False):
        with heavydb.targets['cpu']:
            with Type.alias(**heavydb.typesystem_aliases):
                typ = Type.fromobject(s)
                if caller:
                    typ = heavydb.caller_signature(typ)
                return heavydb.format_type(typ)

    assert test('int32 x') == 'int32 x'
    assert test('Column<int32 x | name=y>') == 'Column<int32 x | name=y>'
    assert test('Column<int32 | name=y>') == 'Column<int32 y>'
    assert test('Column<int32 x>') == 'Column<int32 x>'
    assert test('Column<int32>') == 'Column<int32>'
    assert test('Column<int32> z') == 'Column<int32> z'
    assert test('Column<int32> | name=z') == 'Column<int32> z'
    assert test('Column<int32> x | name=z') == 'Column<int32> x | name=z'
    assert test('Column<TextEncodingNone>') == 'Column<TextEncodingNone>'
    assert test('Column<Array<int32>>') == 'Column<Array<int32>>'
    assert test('Column<Array<TextEncodingNone>>') == 'Column<Array<TextEncodingNone>>'

    assert test('OutputColumn<int32>') == 'OutputColumn<int32>'
    assert test('ColumnList<int32>') == 'ColumnList<int32>'

    assert test('UDTF(ColumnList<int32>)') == 'UDTF(ColumnList<int32>)'
    assert test('int32(ColumnList<int32>)') == 'UDTF(ColumnList<int32>)'
    assert (test('UDTF(int32 x, Column<float32> y, OutputColumn<int64> z)')
            == 'UDTF(int32 x, Column<float32> y, OutputColumn<int64> z)')
    assert test('UDTF(RowMultiplier)') == 'UDTF(RowMultiplier)'
    assert test('UDTF(RowMultiplier m)') == 'UDTF(RowMultiplier m)'
    assert test('UDTF(RowMultiplier | name=m)') == 'UDTF(RowMultiplier m)'
    assert test('UDTF(Constant m)') == 'UDTF(Constant m)'
    assert test('UDTF(ConstantParameter m)') == 'UDTF(ConstantParameter m)'
    assert test('UDTF(SpecifiedParameter m)') == 'UDTF(SpecifiedParameter m)'
    assert test('UDTF(PreFlight m)') == 'UDTF(PreFlight m)'
    assert test('UDTF(TableFunctionManager mgr)') == 'UDTF(TableFunctionManager mgr)'
    assert test('UDTF(Cursor<int32, float64>)') == 'UDTF(Cursor<Column<int32>, Column<float64>>)'
    assert test('UDTF(Cursor<int32 x>)') == 'UDTF(Cursor<Column<int32> x>)'
    assert test('UDTF(Cursor<int32 | name=x>)') == 'UDTF(Cursor<Column<int32> x>)'
    assert test('UDTF(Cursor<TextEncodingNone x>)') == 'UDTF(Cursor<Column<TextEncodingNone> x>)'

    assert test('int32(int32)') == '(int32) -> int32'
    assert test('int32(int32 x)') == '(int32 x) -> int32'
    assert test('int32(Array<int32>)') == '(Array<int32>) -> int32'
    assert test('int32(int32[])') == '(Array<int32>) -> int32'
    assert test('int32(Array<int32> x)') == '(Array<int32> x) -> int32'
    assert test('int32(TextEncodingNone)') == '(TextEncodingNone) -> int32'
    assert test('int32(TextEncodingNone x)') == '(TextEncodingNone x) -> int32'
    assert test('int32(TextEncodingDict)') == '(TextEncodingDict) -> int32'
    assert test('int32(TextEncodingDict x)') == '(TextEncodingDict x) -> int32'
    assert test('int32(Array<TextEncodingNone> x)') == '(Array<TextEncodingNone> x) -> int32'

    def test2(s):
        return test(s, caller=True)

    assert test2('UDTF(int32, OutputColumn<int32>)') == '(int32) -> (Column<int32>)'
    assert test2('UDTF(OutputColumn<int32>)') == '(void) -> (Column<int32>)'
    assert (test2('UDTF(int32 | sizer, OutputColumn<int32>)')
            == '(RowMultiplier) -> (Column<int32>)')
    assert (test2('UDTF(ConstantParameter, OutputColumn<int32>)')
            == '(ConstantParameter) -> (Column<int32>)')
    assert test2('UDTF(SpecifiedParameter, OutputColumn<int32>)') == '(void) -> (Column<int32>)'
    assert test2('UDTF(Constant, OutputColumn<int32>)') == '(void) -> (Column<int32>)'
    assert test2('UDTF(PreFlight, OutputColumn<int32>)') == '(void) -> (Column<int32>)'
    assert test2('UDTF(TableFunctionManager, OutputColumn<int32>)') == '(void) -> (Column<int32>)'
    assert (test2('UDTF(RowMultiplier, OutputColumn<Array<TextEncodingNone>>)')
            == '(RowMultiplier) -> (Column<Array<TextEncodingNone>>)')

    assert test2('UDTF(Cursor<int32>)') == '(Cursor<Column<int32>>) -> void'
    assert test2('UDTF(Cursor<int32 x>)') == '(Cursor<Column<int32> x>) -> void'


def test_reconnect(heavydb):

    heavydb2 = heavydb.reconnect()

    assert heavydb.session_id != heavydb2.session_id
    assert heavydb2.host == heavydb.host
    assert heavydb2.port == heavydb.port


def test_non_admin_user(heavydb):
    heavydb.require_version((5, 9), 'Requires omniscidb 5.9 or newer')

    user = 'rbc_test_non_admin_user'
    password = 'Xy2kq_3lM'
    dbname = 'rbc_test_non_admin_user_db'

    # create user and user's database:
    heavydb.sql_execute(f'DROP DATABASE IF EXISTS {dbname};')
    heavydb.sql_execute(f'DROP USER IF EXISTS "{user}";')

    heavydb.sql_execute(
        f"CREATE USER {user} (password = '{password}', is_super = 'false', can_login='true');")
    heavydb.sql_execute(f"CREATE DATABASE {dbname} (owner = '{user}');")
    heavydb.sql_execute(f"ALTER USER {user} (default_db = '{dbname}');")

    # test the ability to register and use UDFs as a non-admin user:
    userdb = heavydb.reconnect(user=user, password=password, dbname=dbname)

    assert userdb.user == user
    assert userdb.password == password
    assert userdb.dbname == dbname

    @userdb('int(int)')
    def rbc_test_non_admin_user_udf(x):
        return x + 1

    assert rbc_test_non_admin_user_udf(1).execute() == 2

    # clean up:
    heavydb.sql_execute(f'DROP DATABASE IF EXISTS {dbname};')
    heavydb.sql_execute(f'DROP USER IF EXISTS "{user}";')
