import os
from collections import defaultdict
import pytest

rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
if available_version and available_version < (5, 5):
    reason = ("test file requires omniscidb version 5.5+ with bytes support"
              "(got %s)" % (available_version, ))
    available_version = None

pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = os.path.splitext(os.path.basename(__file__))[0]
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))

    sqltypes = ['TEXT ENCODING DICT', 'TEXT ENCODING DICT(16)', 'TEXT ENCODING DICT(8)',
                'TEXT[] ENCODING DICT(32)', 'TEXT ENCODING NONE', 'TEXT ENCODING NONE', 'INT[]']
    colnames = ['t4', 't2', 't1', 's', 'n', 'n2', 'i4']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});'
        .format(**locals()))

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
        m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


def test_table_data(omnisci):
    omnisci.reset()

    descr, result = omnisci.sql_execute(
        'select t4, t2, t1, s, n from {omnisci.table_name}'.format(**locals()))
    result = list(result)

    assert result == [('foofoo', 'foofoo', 'fun', ['foo', 'bar'], 'fun'),
                      ('bar', 'bar', 'bar', ['fun', 'bar'], 'bar'),
                      ('fun', 'fun', 'foo', ['foo'], 'foo'),
                      ('bar', 'bar', 'barr', ['foo', 'bar'], 'barr'),
                      ('foo', 'foo', 'foooo', ['fun', 'bar'], 'foooo')]


def test_bytes_len(omnisci):
    omnisci.reset()

    @omnisci('int32(Bytes, Bytes)')
    def mystrlen(s, s2):
        return len(s) * 100 + len(s2)

    sql_query_expected = f"select length(n) * 100 + length(n2) from {omnisci.table_name}"
    sql_query = f"select mystrlen(n, n2) from {omnisci.table_name}"

    descr, result_expected = omnisci.sql_execute(sql_query_expected)
    result_expected = list(result_expected)
    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    assert result == result_expected


def test_bytes_ord(omnisci):
    omnisci.reset()

    @omnisci('int64(Bytes, int32)', devices=['gpu', 'cpu'])
    def myord(s, i):
        return s[i] if i < len(s) else 0

    sql_query_data = f"select n from {omnisci.table_name}"
    descr, data = omnisci.sql_execute(sql_query_data)
    data = list(data)

    for i in range(5):
        result_expected = [(ord(d[0][i]) if i < len(d[0]) else 0, ) for d in data]
        sql_query = f"select myord(n, {i}) from {omnisci.table_name}"
        descr, result = omnisci.sql_execute(sql_query)
        result = list(result)
        assert result == result_expected


def test_bytes_return(omnisci):
    omnisci.reset()

    from rbc.omnisci_backend import Bytes

    @omnisci('Bytes(int32, int32)')
    def make_abc(first, n):
        r = Bytes(n)
        for i in range(n):
            r[i] = first + i
        return r

    sql_query = "select make_abc(97, 10)"
    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    assert result == [('abcdefghij', )]


def test_bytes_upper(omnisci):
    omnisci.reset()

    from rbc.omnisci_backend import Bytes

    @omnisci('Bytes(Bytes)')
    def myupper(s):
        r = Bytes(len(s))
        for i in range(len(s)):
            c = s[i]
            if c >= 97 and c <= 122:
                c = c - 32
            r[i] = c
        return r

    sql_query = f"select n, myupper(n) from {omnisci.table_name}"
    descr, result = omnisci.sql_execute(sql_query)
    result = list(result)

    for n, u in result:
        assert n.upper() == u
