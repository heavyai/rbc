from rbc.tests import heavydb_fixture
from rbc.heavydb import TextEncodingNone
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
    # [X] set.add
    # [X] set.clear
    # [X] set.copy
    # [X] set.difference
    # [X] set.difference_update
    # [X] set.discard
    # [X] set.intersection
    # [X] set.intersection_update
    # [X] set.isdisjoint
    # [X] set.issubset
    # [X] set.issuperset
    # [X] set.pop
    # [X] set.remove
    # [X] set.symmetric_difference
    # [X] set.symmetric_difference_update
    # [X] set.union
    # [X] set.update

    @heavydb("i32(i32, TextEncodingNone)", devices=['cpu'])
    def test_set(t, method):
        s = set()
        s.add('abc')
        return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_clear(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s) == 0

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_copy(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_difference(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_difference_update(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_discard(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_intersection(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_intersection_update(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_isdisjoint(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_issubset(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_issuperset(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_pop(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_symmetric_difference(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_symmetric_difference_update(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_union(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)

    # @heavydb("i32(i32)", devices=['cpu'])
    # def set_update(t):
    #     s = set()
    #     s.add('abc')
    #     return len(s)


@pytest.mark.parametrize('method,ans', [('add', 1), ('clear', 3), ('copy', 1),
                                        ('difference', 0), ('difference_update', 2),
                                        ('discard', 1), ('intersection', 1),
                                        ('intersection_update', 2), ('isdisjoint', 1),
                                        ('issubset', 1), ('issuperset', 3), ('pop', 0),
                                        ('remove', 0), ('symmetric_difference', 0),
                                        ('symmetric_difference_update', 0),
                                        ('union', 0), ('update', 0)])
def test_set_methods(heavydb, method, ans):
    _, result = heavydb.sql_execute(f"select test_set(1, '{method}');")
    assert list(result)[0] == (ans,)
