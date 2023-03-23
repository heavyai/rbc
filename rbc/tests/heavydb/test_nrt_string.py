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


# missing methods:
# encode
# format
# format_map
# partition
# rpartition
# translate
# maketrans

def define(heavydb):

    @heavydb("TextEncodingNone(TextEncodingNone, TextEncodingNone)", devices=['cpu'])
    def test_string(t, method):
        s = t.to_string()
        if method == 'capitalize':
            r = s.capitalize()
        elif method == 'casefold':
            r = s.casefold()
        elif method == 'lower':
            r = s.lower()
        elif method == 'upper':
            r = s.upper()
        elif method == 'swapcase':
            r = s.swapcase()
        elif method == 'lstrip':
            r = s.lstrip()
        elif method == 'rstrip':
            r = s.rstrip()
        elif method == 'strip':
            r = s.strip()
        elif method == 'title':
            r = s.title()
        else:
            r = ''
        return TextEncodingNone(r)

    @heavydb('bool(TextEncodingNone, TextEncodingNone, TextEncodingNone)')
    def test_endswith(method, t, value):
        s = t.to_string()
        w = value.to_string()
        if method == 'endswith':
            return s.endswith(w)
        elif method == 'startswith':
            return s.startswith(w)
        return False

    @heavydb('TextEncodingNone(TextEncodingNone, TextEncodingNone, TextEncodingNone)')
    def test_replace(t, old, new):
        s = t.to_string()
        o = old.to_string()
        n = new.to_string()
        r = s.replace(o, n)
        return TextEncodingNone(r)

    @heavydb('TextEncodingNone(TextEncodingNone, TextEncodingNone, TextEncodingNone)')
    def test_split(method, string, sep):
        s = string.to_string()
        sep = sep.to_string()

        if method == 'split':
            lst = s.split(sep)
            r = ' '.join(lst)
        elif method == 'rsplit':
            lst = s.rsplit(sep)
            r = ' '.join(lst)
        elif method == 'splitlines':
            lst = s.splitlines()
            r = ' '.join(lst)
        else:
            r = ''
        return TextEncodingNone(r)

    @heavydb('TextEncodingNone(TextEncodingNone, TextEncodingNone, i32)')
    def test_just(method, string, width):
        s = string.to_string()
        r = ''
        if method == 'center':
            r = s.center(width)
        elif method == 'expandtabs':
            r = s.expandtabs(width)
        elif method == 'ljust':
            r = s.ljust(width)
        elif method == 'rjust':
            r = s.rjust(width)
        elif method == 'zfill':
            r = s.zfill(width)
        return TextEncodingNone(r)

    @heavydb('i32(TextEncodingNone, TextEncodingNone, TextEncodingNone)')
    def test_string2(method, t, word):
        s = t.to_string()
        w = word.to_string()
        if method == 'count':
            return s.count(w)
        elif method == 'find':
            return s.find(w)
        elif method == 'rfind':
            return s.rfind(w)
        elif method == 'index':
            return s.index(w)
        elif method == 'rindex':
            return s.rindex(w)
        return -1

    @heavydb('bool(TextEncodingNone, TextEncodingNone)')
    def test_is(method, t):
        s = t.to_string()
        if method == 'isalnum':
            return s.isalnum()
        elif method == 'isalpha':
            return s.isalpha()
        elif method == 'isascii':
            return s.isascii()
        elif method == 'isdecimal':
            return s.isdecimal()
        elif method == 'isdigit':
            return s.isdigit()
        elif method == 'isidentifier':
            return s.isidentifier()
        elif method == 'islower':
            return s.islower()
        elif method == 'isnumeric':
            return s.isnumeric()
        elif method == 'isprintable':
            return s.isprintable()
        elif method == 'isspace':
            return s.isspace()
        elif method == 'istitle':
            return s.istitle()
        elif method == 'isupper':
            return s.isupper()
        return False


tests = [
    ('capitalize', 'heavydb'),
    ('casefold', 'HeavyDB'),
    ('lower', 'PYTHON IS FUN'),
    ('upper', 'python iS Fun'),
    ('swapcase', 'pYtHoN'),
    ('lstrip', '  python'),
    ('rstrip', 'python  '),
    ('strip', '  python  '),
    ('title', 'My favorite number is 25.'),
]


@pytest.mark.parametrize('method,arg', tests)
def test_string_methods(heavydb, method, arg: str):
    _, result = heavydb.sql_execute(f"select test_string('{arg}', '{method}');")

    fn = getattr(arg, method)
    ans = fn()
    assert list(result)[0] == (ans,)


@pytest.mark.parametrize('method,string,word', [
    ('count', 'I love apples, apples are my favorite fruit', 'apple'),
    ('count', 'I love apples, apples are my favorite fruit', 'orange'),
    ('find', 'Hello, welcome to my world.', 'welcome'),
    ('find', 'Hello, welcome to my world.', 'orange'),
    ('rfind', 'Hello, welcome to my world.', 'welcome'),
    ('rfind', 'Hello, welcome to my world.', 'orange'),
    ('index', 'Hello, welcome to my world.', 'welcome'),
    ('rindex', 'Hello, welcome to my world.', 'welcome'),
])
def test_string_two_arg(heavydb, method, string, word):
    _, result = heavydb.sql_execute(f"select test_string2('{method}', '{string}', '{word}')")
    ans = getattr(string, method)(word)
    assert list(result)[0] == (ans,)


@pytest.mark.parametrize('method,string,sep', [
    ('split', 'BMW-Telsa-Range Rover', '-'),
    ('rsplit', 'BMW-Telsa-Range Rover', '-'),
    ('splitlines', 'I\nlove\nPython\nProgramming.', ''),
])
def test_split(heavydb, method, string, sep):
    _, result = heavydb.sql_execute(f"select test_split('{method}', '{string}', '{sep}')")
    if method == 'splitlines':
        r = getattr(string, method)()
    else:
        r = getattr(string, method)(sep)
    ans = ' '.join(r)
    assert list(result)[0] == (ans,)


@pytest.mark.parametrize('method,string,width', [
    ('center', 'banana', 20),
    ('expandtabs', 'H\te\tl\tl\to', 2),
    ('ljust', 'cat', 5),
    ('rjust', 'cat', 5),
    ('zfill', 'cat', 5),
])
def test_string_just(heavydb, method, string, width):
    _, result = heavydb.sql_execute(f"select test_just('{method}', '{string}', {width})")
    ans = getattr(string, method)(width)
    assert list(result)[0] == (ans,)


@pytest.mark.parametrize('method,string,value', [
    ('endswith', 'Hello, welcome to my world.', '.'),
    ('endswith', 'Hello, HeavyDB', 'HeavyAI'),
    ('statswith', 'Hello, HeavyDB', 'Hello'),
    ('startswith', 'Hello, HeavyDB', 'HeavyAI'),
])
def test_string_endswith(heavydb, method, string, value):
    _, result = heavydb.sql_execute(f"select test_endswith('{method}', '{string}', '{value}')")
    ans = string.endswith(value)
    assert list(result)[0] == (ans,)


@pytest.mark.parametrize('string,old,new', [
    ('bat ball', 'ba', 'ro'),
])
def test_replace(heavydb, string, old, new):
    _, result = heavydb.sql_execute(f"select test_replace('{string}', '{old}', '{new}')")
    ans = string.replace(old, new)
    assert list(result)[0] == (ans,)


@pytest.mark.parametrize('method,string', [
    ('isalnum', 'Hello, welcome to my world.'),
    ('isalnum', 'Company12'),
    ('isalpha', 'CompanyX'),
    ('isascii', 'Company123'),
    ('isdecimal', '28212'),
    ('isdecimal', '32ladk3'),
    ('isdecimal', 'Mo3 nicaG el l22er'),
    ('isdigit', '32ladk3'),
    ('isdigit', '342'),
    ('isdigit', 'Python'),
    ('isidentifier', 'Python'),
    ('isidentifier', '22 Python'),
    ('isidentifier', 'Py thon'),
    ('isidentifier', ''),
    ('islower', 'this is good'),
    ('islower', 'th!s is a1so g00d'),
    ('islower', 'this is Not good'),
    ('isnumeric', '523'),
    ('isnumeric', 'Python3'),
    ('isprintable', 'python'),
    ('isprintable', 'python\n'),
    ('isspace', '    \t'),
    ('isspace', ' a '),
    ('isspace', ''),
    ('istitle', 'Python Is Good'),
    ('istitle', 'Python is Good'),
    ('istitle', 'This Is @ Symbol'),
    ('isupper', 'THIS IS GOOD'),
    ('isupper', 'THIS IS not GOOD'),
])
def test_string_is(heavydb, method, string):

    _, result = heavydb.sql_execute(f"select test_is('{method}', '{string}')")
    ans = getattr(string, method)()
    assert list(result)[0] == (ans,)
