__all__ = ['omnisci_fixture']


import os
import pytest
import warnings
from collections import defaultdict
from rbc.utils import version_date, version_hash


def omnisci_fixture(caller_globals, minimal_version=(0, 0),
                    suffices=['', '10', 'null', 'array', 'arraynull'],
                    load_columnar=True, debug=False):
    """Usage from a rbc/tests/test_xyz.py file:

    .. code-block:: python

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

    f'{omnisci.table_name}array' - contains arrays f8, f4, i8, i4, i2,
                                   i1, b with row size 5

    f'{omnisci.table_name}arraynull' - contains arrays f8, f4, i8, i4, i2,
                                       i1, b with row size 5, contains null
                                       values.
    """
    rbc_omnisci = pytest.importorskip('rbc.omniscidb')
    available_version, reason = rbc_omnisci.is_available()

    def require_version(version, message=None, date=None, hash=None):
        # version date is the build data while version hash determines
        # the merge date of a feature, see rbc issue 332
        hash_date_map = {
            '4777a06b01': 20210429.5,  # PR 5465
            '758cf7a61a': 20210429,    # v5.7.0dev-20210429
            '9dbd553c44': 20210329,    # v5.5.2
            '5b4ddfcdcd': 20210112,    # v5.4.1
        }
        if not available_version:
            pytest.skip(reason)
        assert isinstance(version, tuple)
        if available_version < version:
            _reason = f'test requires version {version} or newer, got {available_version}'
            if message is not None:
                _reason += f': {message}'
            pytest.skip(_reason)
        available_hash = version_hash(available_version)
        available_date = hash_date_map.get(available_hash)
        if hash is not None:
            date = hash_date_map.get(hash, date)
        if date is not None:
            assert isinstance(date, (int, float))
            if available_date is None:
                available_date = version_date(available_version)
            if not available_date:
                warnings.warn('could not determine date of {available_version}')
                return
            if available_date < date:
                required_version = f'{".".join(map(str, version))}-{date or "*"}-{hash or "*"}'
                current_version = (f'{".".join(map(str, available_version[:3]))}'
                                   f'-{available_date or "*"}-{available_hash or "*"}')
                _reason = f'version {required_version} or newer required, got {current_version}'
                if message is not None:
                    _reason += f': {message}'
                pytest.skip(_reason)

    # Throw an error on Travis CI if the server is not available
    if "TRAVIS" in os.environ and not available_version:
        pytest.exit(msg=reason, returncode=1)

    require_version(minimal_version)

    filename = caller_globals['__file__']
    table_name = os.path.splitext(os.path.basename(filename))[0]

    config = rbc_omnisci.get_client_config(debug=debug)
    m = rbc_omnisci.RemoteOmnisci(**config)

    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN']
    arrsqltypes = [t + '[]' for t in sqltypes]
    # todo: TEXT ENCODING DICT, TEXT ENCODING NONE, TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://www.omnisci.com/docs/latest/5_datatypes.html
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    arrtable_defn = ',\n'.join('%s %s' % (n, t)
                               for t, n in zip(arrsqltypes, colnames))

    for suffix in suffices:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}{suffix}')
        if 'array' in suffix:
            m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}{suffix} ({arrtable_defn});')
        else:
            m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}{suffix} ({table_defn});')

    if load_columnar:
        # fast method using load_table_columnar thrift endpoint, use for large tables
        def row_value(row, col, colname, null=False, arr=False):
            if arr:
                if null and (0 == (row + col) % 2):
                    return None
                a = [row_value(row + i, col, colname, null=null, arr=False) for i in range(row)]
                return a
            if null and (0 == (row + col) % 3):
                return None
            if colname == 'b':
                return row % 2 == 0
            return row

        for suffix in suffices:
            columns = defaultdict(list)
            for j, n in enumerate(colnames):
                for i in range(10 if '10' in suffix else 5):
                    v = row_value(i, j, n, null=('null' in suffix), arr=('array' in suffix))
                    columns[n].append(v)
            m.load_table_columnar(f'{table_name}{suffix}', **columns)

    else:
        # slow method using SQL query statements
        def row_value(row, col, colname, null=False, arr=False):
            if arr:
                if null and (0 == (row + col) % 2):
                    return 'NULL'
                a = [row_value(row + i, col, colname, null=null, arr=False) for i in range(row)]
                return '{' + ', '.join(map(str, a)) + '}'
            if null and (0 == (row + col) % 3):
                return 'NULL'
            if colname == 'b':
                return ("'true'" if row % 2 == 0 else "'false'")
            return row

        for i in range(10):
            if i < 5:
                for suffix in suffices:
                    if suffix == '':
                        table_row = ', '.join(str(row_value(i, j, n))
                                              for j, n in enumerate(colnames))
                    elif suffix == 'null':
                        table_row = ', '.join(str(row_value(i, j, n, null=True))
                                              for j, n in enumerate(colnames))
                    elif suffix == 'array':
                        table_row = ', '.join(str(row_value(i, j, n, arr=True))
                                              for j, n in enumerate(colnames))
                    elif suffix == 'arraynull':
                        table_row = ', '.join(str(row_value(i, j, n, null=True, arr=True))
                                              for j, n in enumerate(colnames))
                    else:
                        continue
                    m.sql_execute(f'INSERT INTO {table_name}{suffix} VALUES ({table_row})')
            if i < 10 and '10' in suffices:
                table_row = ', '.join(str(row_value(i, j, n)) for j, n in enumerate(colnames))
                m.sql_execute(f'INSERT INTO {table_name}10 VALUES ({table_row})')

    m.table_name = table_name
    m.require_version = require_version
    yield m
    for suffix in suffices:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}{suffix}')
