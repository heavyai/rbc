import pytest
import re
from rbc.tests import omnisci_fixture
from rbc.errors import OmnisciServerError


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 6)):
        yield o


@pytest.mark.parametrize("func", ['db_scan', 'kmeans'])
def test_mlpack(omnisci, func):
    omnisci.require_version(
        (5, 6),
        'Requires omniscidb-internal PR 5430 and omniscidb built with -DENABLE_MLPACK',
        date=20210401)

    extra_args = dict(db_scan='cast(1 as float), 1',
                      kmeans='1')[func]
    query = (f'select * from table({func}(cursor(select cast(rowid as int), f8, f8, f8 '
             f'from {omnisci.table_name}), {extra_args}, 1))')

    try:
        _, result = omnisci.sql_execute(query)
    except OmnisciServerError as msg:
        m = re.match(r'.*No match found for function signature ' + func + r'[(]',
                     msg.args[0])
        if m is not None:
            pytest.skip(f'test requires omniscidb server with MLPACK support: {msg}')
        raise

    result = list(result)

    expected = dict(
        db_scan=[(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)],
        kmeans=[(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)]
    )[func]

    assert result == expected
