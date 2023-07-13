import pytest

from rbc.heavydb import TextEncodingNone
from rbc.tests import heavydb_fixture

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['text']):
        yield o


def test_TextEncodingNone_len(heavydb):
    heavydb.reset()

    @heavydb('int32(TextEncodingNone, TextEncodingNone)')
    def mystrlen(s, s2):
        return len(s) * 100 + len(s2)

    sql_query_expected = f"select length(n) * 100 + length(n2) from {heavydb.table_name}text"
    sql_query = f"select mystrlen(n, n2) from {heavydb.table_name}text"

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

    sql_query_data = f"select n from {heavydb.table_name}text"
    descr, data = heavydb.sql_execute(sql_query_data)
    data = list(data)

    for i in range(5):
        result_expected = [(ord(d[0][i]) if i < len(d[0]) else 0, ) for d in data]
        sql_query = f"select myord(n, {i}) from {heavydb.table_name}text"
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

    sql_query = f"select n, myupper(n) from {heavydb.table_name}text"
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
    heavydb.require_version((5, 8), 'Requires heavydb 5.8 or newer')

    heavydb.reset()

    @heavydb('int32(TextEncodingNone, TextEncodingNone)')
    def eq1(a, b):
        return a == b

    @heavydb('int32(TextEncodingNone)', devices=['CPU'])
    def eq2(a):
        return a == 'world'

    assert eq1('hello', 'hello').execute() == 1
    assert eq1('c', 'c').execute() == 1
    assert eq1('hello', 'h').execute() == 0
    assert eq1('hello', 'hello2').execute() == 0

    assert eq2('world').execute() == 1


def test_TextEncodingNone_ne(heavydb):
    heavydb.require_version((5, 8), 'Requires heavydb 5.8 or newer')

    heavydb.reset()

    @heavydb('int32(TextEncodingNone, TextEncodingNone)')
    def ne1(a, b):
        return a != b

    @heavydb('int32(TextEncodingNone)', devices=['CPU'])
    def ne2(a):
        return a != 'world'

    assert ne1('hello', 'hello').execute() == 0
    assert ne1('c', 'c').execute() == 0
    assert ne1('hello', 'h').execute() == 1
    assert ne1('hello', 'hello2').execute() == 1

    assert ne2('world').execute() == 0


def test_if_else_assignment(heavydb):
    heavydb.require_version((5, 8), 'Requires heavydb 5.8 or newer')

    # fix for issue #487

    @heavydb("TextEncodingNone(double)")
    def classify_slope(slope):
        if slope <= 5:
            res = TextEncodingNone("low")
        elif 5 < slope < 15:
            res = TextEncodingNone("med")
        else:
            res = TextEncodingNone("high")
        return res

    assert classify_slope(2.4).execute() == "low"
    assert classify_slope(5.4).execute() == "med"
    assert classify_slope(15.4).execute() == "high"


def test_return_text(heavydb):
    @heavydb("TextEncodingNone(TextEncodingNone)", devices=['cpu'])
    def fn(t):
        return t

    table = heavydb.table_name + 'text'
    _, result = heavydb.sql_execute(f"select n, fn(n) from {table};")
    result = list(zip(*result))
    expected, got = result
    assert expected == got


def test_to_string(heavydb):

    @heavydb("TextEncodingNone(TextEncodingNone)", devices=['cpu'])
    def to_string(t):
        s = t.to_string()
        return TextEncodingNone(s)

    table = heavydb.table_name + 'text'
    _, result = heavydb.sql_execute(f"select n, to_string(n) from {table};")
    result = list(zip(*result))
    expected, got = result
    assert expected == got
