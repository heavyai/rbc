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
    # [ ] set.union
    # [X] set.update

    @heavydb("i32(i32, TextEncodingNone)", devices=['cpu'])
    def test_set(t, method):
        s = set([1, 2, 3, 4, 5])
        if method == 'add':
            return len(s)
        elif method == 'clear':
            s.clear()
            return len(s) == 0
        elif method == 'copy':
            s2 = s.copy()
            return len(s) == len(s2) == 5
        elif method == 'difference':
            s2 = set([1, 3, 5])
            return s.difference(s2) == {2, 4}
        elif method == 'difference_update':
            s.difference_update({1, 2, 3})
            return len(s) == 2
        elif method == 'discard':
            s.discard(1)
            return len(s) == 4
        elif method == 'intersection':
            return s.intersection({1, 3, 5}) == {1, 3, 5}
        elif method == 'intersection_update':
            s.intersection_update({1, 2})
            return len(s) == 2
        elif method == 'isdisjoint':
            return s.isdisjoint({0, 6})
        elif method == 'issubset':
            return s.issubset({0, 1, 2, 3, 4, 5})
        elif method == 'issuperset':
            return s.issuperset({1, 2, 3})
        elif method == 'pop':
            s.pop()
            return len(s) == 4
        elif method == 'remove':
            s.remove(2)
            return len(s) == 4
        elif method == 'symmetric_difference':
            s2 = {1, 2}
            return len(s.symmetric_difference(s2)) == 3
        elif method == 'symmetric_difference_update':
            s.symmetric_difference_update({0, 6})
            return len(s) == 7
        # elif method == 'union':
        #     return len(s.union({0})) == 6
        elif method == 'update':
            s.update({0})
            return len(s) == 6
        return 0


@pytest.mark.parametrize('method,ans', [('add', 5), ('clear', 1), ('copy', 1),
                                        ('difference', 1), ('difference_update', 1),
                                        ('discard', 1), ('intersection', 1),
                                        ('intersection_update', 1), ('isdisjoint', 1),
                                        ('issubset', 1), ('issuperset', 1), ('pop', 1),
                                        ('remove', 1), ('symmetric_difference', 1),
                                        ('symmetric_difference_update', 1),
                                        ('union', 1), ('update', 1)])
def test_set_methods(heavydb, method, ans):
    skip_list = ('union',)
    if method in skip_list:
        pytest.skip(method)
    _, result = heavydb.sql_execute(f"select test_set(1, '{method}');")
    assert list(result)[0] == (ans,)
