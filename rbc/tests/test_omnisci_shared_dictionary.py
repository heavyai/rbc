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
    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')

    m.sql_execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            base TEXT ENCODING DICT(32),
            derived TEXT,
            SHARED DICTIONARY (derived) REFERENCES {table_name}(base)
        );
    ''')

    data = {
        "base": ["hello", "foo", "foofoo", "world", "bar", "foo", "foofoo"],
        "derived": ["world", "bar", "hello", "foo", "baz", "hello", "foo"]
    }

    m.load_table_columnar(table_name, **data)
    m.table_name = table_name

    yield m
    try:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


def test_data(omnisci):
    omnisci.reset()

    _, result = omnisci.sql_execute(
        f'select * from {omnisci.table_name}')

    result = list(result)

    assert result == [('hello', 'world'),
                      ('foo', 'bar'),
                      ('foofoo', 'Hello'),
                      ('world', 'foo'),
                      ('bar', 'baz')]


def test_fx(omnisci):

    fn = "ct_binding_dict_encoded1"
    table = omnisci.table_name
    
    _, result = omnisci.sql_execute(
        f"SELECT base, key_for_string(base) FROM {table};")
    print(list(result))

    query = (
        f"SELECT * FROM table({fn}("
        f" cursor(SELECT base FROM {table}),"
         " 1));"
    )
    print(query)

    _, result = omnisci.sql_execute(query)
    print(list(result))
