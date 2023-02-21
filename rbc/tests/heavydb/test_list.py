from rbc.tests import heavydb_fixture
import pytest

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), load_test_data=False):
        define(o)
        yield o

def define(heavydb):
    # Defines simple methods just to verify if NRT is working and if any there
    # are any symbol
    #
    # List of methods:
    # [X] List.append
    # [X] List.extend
    # [X] List.insert
    # [X] List.remove
    # [X] List.pop
    # [X] List.clear
    # [X] List.index
    # [X] List.count
    # [X] List.sort
    # [X] List.reverse
    # [X] List.copy

    @heavydb("i32(i32)", devices=['cpu'])
    def list_append(t):
        l = list()
        l.append('abc')
        return len(l)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_extend(t):
        l = list()
        l.extend('abc')
        return len(l)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_insert(t):
        l = list()
        l.insert(0, t)
        return len(l)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_remove(t):
        l = list()
        l.extend([t+1, t, t+2])
        l.remove(t)
        return t in l

    @heavydb("i32(i32)", devices=['cpu'])
    def list_pop(t):
        l = list()
        l.extend([t, t, t])
        l.pop()
        return len(l)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_clear(t):
        l = list()
        l.extend([t, t, t])
        l.clear()
        return len(l) == 0

    @heavydb("i32(i32)", devices=['cpu'])
    def list_index(t):
        l = list()
        l.extend([t-1, t, t+1])
        return l.index(t)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_count(t):
        l = list()
        l.extend([t-1, t, t, t+1])
        return l.count(t)

    @heavydb("bool(i32)", devices=['cpu'])
    def list_sort(t):
        l = list()
        l.extend([t+1, t, t-1])
        l.sort()
        return l[0] < l[1] and l[1] < l[2]

    @heavydb("bool(i32)", devices=['cpu'])
    def list_reverse(t):
        l = list()
        l.extend([t+1, t, t-1])
        l.reverse()
        return l[0] < l[1] and l[1] < l[2]

    @heavydb("i32(i32)", devices=['cpu'])
    def list_copy(t):
        l = list()
        l.extend([t-1, t, t+1])
        l2 = l.copy()
        return len(l2)


@pytest.mark.parametrize('method,ans', [('append', 1), ('extend', 3), ('insert', 1),
                                    ('remove', 0), ('pop', 2), ('clear', 1),
                                    ('index', 1), ('count', 2), ('sort', 1),
                                    ('reverse', 1), ('copy', 3)])
def test_list_methods(heavydb, method, ans):
    _, result = heavydb.sql_execute(f"select list_{method}(1);")
    assert list(result)[0] == (ans,)
