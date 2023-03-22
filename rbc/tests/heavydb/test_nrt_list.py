from rbc.tests import heavydb_fixture
from rbc.heavydb import Array
import pytest

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(),
                             suffices=['array', 'arraynull']):
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
        lst = list()
        lst.append('abc')
        return len(lst)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_extend(t):
        lst = list()
        lst.extend('abc')
        return len(lst)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_insert(t):
        lst = list()
        lst.insert(0, t)
        return len(lst)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_remove(t):
        lst = list()
        lst.extend([t+1, t, t+2])
        lst.remove(t)
        return t in lst

    @heavydb("i32(i32)", devices=['cpu'])
    def list_pop(t):
        lst = list()
        lst.extend([t, t, t])
        lst.pop()
        return len(lst)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_clear(t):
        lst = list()
        lst.extend([t, t, t])
        lst.clear()
        return len(lst) == 0

    @heavydb("i32(i32)", devices=['cpu'])
    def list_index(t):
        lst = list()
        lst.extend([t-1, t, t+1])
        return lst.index(t)

    @heavydb("i32(i32)", devices=['cpu'])
    def list_count(t):
        lst = list()
        lst.extend([t-1, t, t, t+1])
        return lst.count(t)

    @heavydb("bool(i32)", devices=['cpu'])
    def list_sort(t):
        lst = list()
        lst.extend([t+1, t, t-1])
        lst.sort()
        return lst[0] < lst[1] and lst[1] < lst[2]

    @heavydb("bool(i32)", devices=['cpu'])
    def list_reverse(t):
        lst = list()
        lst.extend([t+1, t, t-1])
        lst.reverse()
        return lst[0] < lst[1] and lst[1] < lst[2]

    @heavydb("i32(i32)", devices=['cpu'])
    def list_copy(t):
        lst = list()
        lst.extend([t-1, t, t+1])
        l2 = lst.copy()
        return len(l2)


@pytest.mark.parametrize('method,ans', [('append', 1), ('extend', 3), ('insert', 1),
                                        ('remove', 0), ('pop', 2), ('clear', 1),
                                        ('index', 1), ('count', 2), ('sort', 1),
                                        ('reverse', 1), ('copy', 3)])
def test_list_methods(heavydb, method, ans):
    _, result = heavydb.sql_execute(f"select list_{method}(1);")
    assert list(result) == [(ans,)]


@pytest.mark.parametrize('strategy', ['array', 'arraynull'])
def test_to_list(heavydb, strategy):

    @heavydb('int64[](int64[])', devices=['cpu'])
    def to_list(a):
        if a.is_null():
            other = Array(0, 'int64')
            other.set_null()
            return other
        else:
            lst = a.to_list()
            return Array(lst)

    heavydb.register()
    table = heavydb.table_name + strategy

    _, result = heavydb.sql_execute(f'select i8, to_list(i8) from {table}')
    result = list(zip(*result))
    expected, got = result
    for e, g in zip(expected, got):
        if e == []:
            # It is not possible to distinguish an empty list created by a null
            # array from one created by an empty array
            #   list(null array) -> []
            #   list(empty array) -> []
            assert g is None
        else:
            assert e == g
