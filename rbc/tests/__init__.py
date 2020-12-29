__all__ = ['omnisci_fixture']


import os
import pytest


def omnisci_fixture(caller_globals, minimal_version=(0, 0)):
    """Usage from a rbc/tests/test_xyz.py file:

      import pytest
      from rbc.tests import omnisci_fixture
      @pytest.fixture(scope='module')
      def omnisci():
          from o in omnisci_fixture(globals()):
              # do some customization here
              yield o

    This fixture creates the following tables:

    f'{omnisci.table_name}' - contains columns f8, f4, i8, i4, i2, i1,
                              b with row size 5.

    f'{omnisci.table_name}10' - contains columns f8, f4, i8, i4, i2,
                                i1, b with row size 10.

    f'{omnisci.table_name}null' - contains columns f8, f4, i8, i4, i2,
                                  i1, b with row size 5, contains null
                                  values.
    """
    rbc_omnisci = pytest.importorskip('rbc.omniscidb')
    available_version, reason = rbc_omnisci.is_available()

    def require_version(version, message=None):
        if not available_version:
            pytest.skip(reason)
        if available_version < version:
            _reason = f'test requires version {version} or newer, got {available_version}'
            if message is not None:
                _reason += f': {message}'
            pytest.skip(_reason)

    # Throw an error on Travis CI if the server is not available
    if "TRAVIS" in os.environ and not available_version:
        pytest.exit(msg=reason, returncode=1)

    require_version(minimal_version)

    filename = caller_globals['__file__']
    table_name = os.path.splitext(os.path.basename(filename))[0]

    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)

    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}10')
    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}null')
    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN']
    # todo: TEXT ENCODING DICT, TEXT ENCODING NONE, TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://www.omnisci.com/docs/latest/5_datatypes.html
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});')
    m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}10 ({table_defn});')
    m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}null ({table_defn});')

    def row_value(row, col, colname, null=False):
        if null:
            return 'NULL'
        if colname == 'b':
            return ("'true'" if row % 2 == 0 else "'false'")
        return row

    for i in range(10):
        if i < 5:
            table_row = ', '.join(str(row_value(i, j, n)) for j, n in enumerate(colnames))
            m.sql_execute(f'INSERT INTO {table_name} VALUES ({table_row})')
            table_row = ', '.join(str(row_value(i, j, n, null=(0 == (i + j) % 3)))
                                  for j, n in enumerate(colnames))
            m.sql_execute(f'INSERT INTO {table_name}null VALUES ({table_row})')
        if i < 10:
            table_row = ', '.join(str(row_value(i, j, n)) for j, n in enumerate(colnames))
            m.sql_execute(f'INSERT INTO {table_name}10 VALUES ({table_row})')

    m.table_name = table_name
    m.require_version = require_version
    yield m
    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}')
    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}10')
    m.sql_execute(f'DROP TABLE IF EXISTS {table_name}null')
