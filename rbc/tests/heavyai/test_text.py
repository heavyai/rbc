import os
from collections import defaultdict
from rbc.heavyai import TextEncodingNone
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

    sqltypes = ['TEXT ENCODING DICT', 'TEXT ENCODING DICT(16)', 'TEXT ENCODING DICT(8)',
                'TEXT[] ENCODING DICT(32)', 'TEXT ENCODING NONE', 'TEXT ENCODING NONE', 'INT[]']
    colnames = ['t4', 't2', 't1', 's', 'n', 'n2', 'i4']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});')

    data = defaultdict(list)
    for i in range(5):
        for j, n in enumerate(colnames):
            # todo: to check is_null, use empty string
            if n in ['t4', 't2']:
                data[n].append(['foofoo', 'bar', 'fun', 'bar', 'foo'][i])
            if n in ['t1', 'n']:
                data[n].append(['fun', 'bar', 'foo', 'barr', 'foooo'][i])
            elif n == 's':
                data[n].append([['foo', 'bar'], ['fun', 'bar'], ['foo']][i % 3])
            elif n == 'n2':
                data[n].append(['1', '12', '123', '1234', '12345'][i])
            elif n == 'i4':
                data[n].append(list(range(i+1)))

    m.load_table_columnar(table_name, **data)
    m.table_name = table_name
    yield m
    try:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


def test_table_data(heavydb):
    heavydb.reset()

    descr, result = heavydb.sql_execute(
        f'select t4, t2, t1, s, n from {heavydb.table_name}')
    result = list(result)

    assert result == [('foofoo', 'foofoo', 'fun', ['foo', 'bar'], 'fun'),
                      ('bar', 'bar', 'bar', ['fun', 'bar'], 'bar'),
                      ('fun', 'fun', 'foo', ['foo'], 'foo'),
                      ('bar', 'bar', 'barr', ['foo', 'bar'], 'barr'),
                      ('foo', 'foo', 'foooo', ['fun', 'bar'], 'foooo')]


def test_TextEncodingNone_len(heavydb):
    heavydb.reset()

    @heavydb('int32(TextEncodingNone, TextEncodingNone)')
    def mystrlen(s, s2):
        return len(s) * 100 + len(s2)

    sql_query_expected = f"select length(n) * 100 + length(n2) from {heavydb.table_name}"
    sql_query = f"select mystrlen(n, n2) from {heavydb.table_name}"

    descr, result_expected = heavydb.sql_execute(sql_query_expected)
    result_expected = list(result_expected)
    descr, result = heavydb.sql_execute(sql_query)
    result = list(result)

    assert result == result_expected


def test_TextEncodingNone_ord(heavydb):
    heavydb.reset()

    @heavydb('int64(TextEncodingNone, int32)', devices=['gpu', 'cpu'])
    def myord(s, i):
        return s[i] if i < len(s) else 0

    sql_query_data = f"select n from {heavydb.table_name}"
    descr, data = heavydb.sql_execute(sql_query_data)
    data = list(data)

    for i in range(5):
        result_expected = [(ord(d[0][i]) if i < len(d[0]) else 0, ) for d in data]
        sql_query = f"select myord(n, {i}) from {heavydb.table_name}"
        descr, result = heavydb.sql_execute(sql_query)
        result = list(result)
        assert result == result_expected


def test_TextEncodingNone_return(heavydb):
    heavydb.reset()

    @heavydb('TextEncodingNone(int32, int32)')
    def make_abc(first, n):
        r = TextEncodingNone(n)
        for i in range(n):
            r[i] = first + i
        return r

    sql_query = "select make_abc(97, 10)"
    descr, result = heavydb.sql_execute(sql_query)
    result = list(result)

    assert result == [('abcdefghij', )]


def test_TextEncodingNone_upper(heavydb):
    heavydb.reset()

    @heavydb('TextEncodingNone(TextEncodingNone)')
    def myupper(s):
        r = TextEncodingNone(len(s))
        for i in range(len(s)):
            c = s[i]
            if c >= 97 and c <= 122:
                c = c - 32
            r[i] = c
        return r

    sql_query = f"select n, myupper(n) from {heavydb.table_name}"
    descr, result = heavydb.sql_execute(sql_query)
    result = list(result)

    for n, u in result:
        assert n.upper() == u


def test_TextEncodingNone_str_constructor(heavydb):
    heavydb.reset()

    @heavydb('TextEncodingNone(int32)')
    def constructor(_):
        return TextEncodingNone('hello world')

    assert constructor(3).execute() == 'hello world'


def test_TextEncodingNone_eq(heavydb):

    heavydb.reset()

    @heavydb('int32(TextEncodingNone, TextEncodingNone)')
    def eq1(a, b):
        return a == b

    @heavydb('int32(TextEncodingNone)')
    def eq2(a):
        return a == 'world'

    assert eq1('hello', 'hello').execute() == 1
    assert eq1('c', 'c').execute() == 1
    assert eq1('hello', 'h').execute() == 0
    assert eq1('hello', 'hello2').execute() == 0

    assert eq2('world').execute() == 1


def test_TextEncodingNone_ne(heavydb):

    heavydb.reset()

    @heavydb('int32(TextEncodingNone, TextEncodingNone)')
    def ne1(a, b):
        return a != b

    @heavydb('int32(TextEncodingNone)')
    def ne2(a):
        return a != 'world'

    assert ne1('hello', 'hello').execute() == 0
    assert ne1('c', 'c').execute() == 0
    assert ne1('hello', 'h').execute() == 1
    assert ne1('hello', 'hello2').execute() == 1

    assert ne2('world').execute() == 0
