import pytest
import re
from rbc.tests import heavydb_fixture
from rbc.errors import HeavyDBServerError


@pytest.fixture(scope='module')
def heavydb():

    for o in heavydb_fixture(globals(), minimal_version=(6, 1), suffices=['']):
        yield o


@pytest.mark.parametrize("func", ['dbscan', 'kmeans'])
def test_mlpack(heavydb, func):
    heavydb.require_version(
        (6, 1),
        'Requires heavydb-internal PR 5430 and heavydb built with -DENABLE_MLPACK')

    extra_args = dict(dbscan='cast(1 as float), 1, \'mlpack\'',
                      kmeans='1, 1, \'default\', \'mlpack\'')[func]
    query = (f'select * from table({func}(cursor(select cast(rowid as int), f8, f8, f8 '
             f'from {heavydb.table_name}), {extra_args}))')

    try:
        _, result = heavydb.sql_execute(query)
    except HeavyDBServerError as msg:
        m = re.match(r'.*Cannot find (mlpack|DEFAULT) ML library to support', msg.args[0])
        if m:
            pytest.skip(f'test requires heavydb server with MLPACK support: {msg}')
        # heavydb from conda-forge is built without MLPACK
        m = re.match(r".*Undefined function call '(kmeans|dbscan)' in SQL statement.*",
                     msg.args[0])
        if m:
            pytest.skip(f'test requires heavydb server with MLPACK support: {msg}')
        raise

    result = list(result)

    expected = dict(
        dbscan=[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)],
        kmeans=[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)]
    )[func]

    assert result == expected
