__all__ = ['heavydb_fixture', 'sql_execute']


import os
import pytest
import warnings
import numpy
from collections import defaultdict


def assert_equal(actual, desired):
    """Test equality of actual and desired.

    When both inputs are numpy array or number objects, test equality
    of dtype attributes as well.
    """
    numpy.testing.assert_equal(actual, desired)

    if isinstance(actual, numpy.ndarray) and isinstance(desired, numpy.ndarray):
        numpy.testing.assert_equal(actual.dtype, desired.dtype)
    elif isinstance(actual, numpy.number) and isinstance(desired, numpy.number):
        numpy.testing.assert_equal(actual.dtype, desired.dtype)


def sql_execute(query):
    """Execute a SQL statement to heavydb server using global instance.

    Use when the query does not require registration of new UDF/UDTFs.
    """
    rbc_heavydb = pytest.importorskip('rbc.heavydb')
    heavydb = next(rbc_heavydb.global_heavydb_singleton)
    return heavydb.sql_execute(query)


def heavydb_fixture(caller_globals, minimal_version=(0, 0),
                    suffices=['', '10', 'null', 'array', 'arraynull', 'text'],
                    load_columnar=True, load_test_data=True, debug=False):
    """Usage from a rbc/tests/test_xyz.py file:

    .. code-block:: python

       import pytest
       from rbc.tests import heavydb_fixture

       @pytest.fixture(scope='module')
       def heavydb():
           from o in heavydb_fixture(globals()):
               # do some customization here
               yield o

    This fixture creates the following tables:

    f'{heavydb.table_name}' - contains columns f8, f4, i8, i4, i2, i1,
                              b with row size 5.

    f'{heavydb.table_name}10' - contains columns f8, f4, i8, i4, i2,
                                i1, b with row size 10.

    f'{heavydb.table_name}null' - contains columns f8, f4, i8, i4, i2,
                                  i1, b with row size 5, contains null
                                  values

    f'{heavydb.table_name}array' - contains arrays f8, f4, i8, i4, i2,
                                   i1, b with row size 5

    f'{heavydb.table_name}arraynull' - contains arrays f8, f4, i8, i4, i2,
                                       i1, b with row size 5, contains null
                                       values.

    f'{heavydb.table_name}text' - contains text t4, t2, t1, s, n
                                  where 't' prefix is for text encoding dict
                                  and 'n' is for text encoding none.
    """
    rbc_heavydb = pytest.importorskip('rbc.heavydb')
    available_version, reason = rbc_heavydb.is_available()

    def require_version(version, message=None, label=None):
        """Execute pytest.skip(...) if version is older than available_version.

        Some tests can be run only when using heavydb server built
        from a particular branch of heavydb.  So, when the specified
        version and the heavydb version match exactly and these
        correspond to the current development version, if the
        specified label does not match with the value of envrinment
        variable HEAVYDB_DEV_LABEL, then the corresponing test will
        be skipped. Use label 'docker-dev' when using heavydb dev
        docker image.

        """
        # The available version (of the heavydb server) has date and
        # hash bits, however, these are useless for determining the
        # version ordering (in the case of available_version[:3] ==
        # version) because the date corresponds to the date of
        # building the server and the hash corresponds to some commit
        # of some repository (heavydb or heavydb-internal) and it
        # does not provide easy date information.
        #
        # The condition available_version[:3] == version can appear in
        # the following cases (given in the order of from newer to
        # older):
        # 1. heavydb is built against a heavydb-internal PR branch
        # (assuming it is rebased against master)
        # 2. heavydb is built against heavydb-internal master branch
        # 3. heavydb is built against heavydb master branch
        # 4. heavydb is built against heavydb dev docker
        # 5. heavydb is built against heavydb/heavydb-internal release tag
        #
        # rbc testing suite may use features that exists in the head
        # of the above list but not in the tail of it. So we have a
        # problem of deciding if a particular test should be disabled
        # or not for a given case while there is no reliable way to
        # tell from heavydb version if the server has the particular
        # feature or not. To resolve this, we use label concept as
        # explained in the doc-string.
        #

        if not available_version:
            pytest.skip(reason)
        # Requires update when heavydb-internal bumps up version number:
        current_development_version = (6, 1, 0)
        if available_version[:3] > current_development_version:
            warnings.warn(f'{available_version}) is newer than development version'
                          f' ({current_development_version}), please update the latter!')

        assert isinstance(version, tuple)
        if version > available_version[:3]:
            _reason = f'test requires version {version} or newer, got {available_version}'
            if message is not None:
                _reason += f': {message}'
            pytest.skip(_reason)

        if label is not None:
            env_label = os.environ.get('HEAVYDB_DEV_LABEL')
            if env_label and label == 'docker-dev':
                # docker-dev is some older master, so it must work
                # with the current master as well as with branches based on the current master.
                label = 'master'
            if label == 'master' and env_label and env_label != 'docker-dev':
                # assuming that the branch given in the label is
                # up-to-date with respect to master branch. If it is
                # not, one should rebase the branch against the
                # master.
                label = env_label
            if env_label is None:
                warnings.warn('Environment does not specify label (HEAVYDB_DEV_LABEL is unset).'
                              ' Tests with development labels will not be run.')
            if env_label != label:
                _reason = (f'test requires version {version} with label {label},'
                           f' got {available_version} with label {env_label}')
                if message is not None:
                    _reason += f': {message}'
                pytest.skip(_reason)

            if version < available_version[:3]:
                # in the case the branch given in the label was never
                # merged, consider removing the corresponding test
                warnings.warn(f'detected test requiring {version} with out-of-date label {label}.'
                              ' Please reset test label to None.')

    # Throw an error on Travis CI if the server is not available
    if "TRAVIS" in os.environ and not available_version:
        pytest.exit(msg=reason, returncode=1)

    require_version(minimal_version)

    filename = caller_globals['__file__']
    table_name = os.path.splitext(os.path.basename(filename))[0]

    config = rbc_heavydb.get_client_config(debug=debug)
    m = rbc_heavydb.RemoteHeavyDB(**config)

    if not load_test_data:
        yield m
        return

    sqltypes = ['FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN']
    arrsqltypes = [t + '[]' for t in sqltypes]
    # todo: TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://docs.heavy.ai/sql/data-definition-ddl/datatypes-and-fixed-encoding
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
    textsqltypes = ['TEXT ENCODING DICT(32)', 'TEXT ENCODING DICT(16)',
                    'TEXT ENCODING DICT(8)', 'TEXT[] ENCODING DICT(32)',
                    'TEXT ENCODING NONE', 'TEXT ENCODING NONE']
    textcolnames = ['t4', 't2', 't1', 's', 'n', 'n2']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    arrtable_defn = ',\n'.join('%s %s' % (n, t)
                               for t, n in zip(arrsqltypes, colnames))
    texttable_defn = ',\n'.join('%s %s' % (n, t)
                                for t, n in zip(textsqltypes, textcolnames))

    for suffix in suffices:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}{suffix}')
        if 'array' in suffix:
            m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}{suffix} ({arrtable_defn})')
        if 'text' in suffix:
            m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}{suffix} ({texttable_defn})')
        else:
            m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}{suffix} ({table_defn})')

    if load_columnar:
        # fast method using load_table_columnar thrift endpoint, use for large tables
        def row_value(row, col, colname, null=False, arr=False, text=False):
            if text:
                if colname in ['t4', 't2']:
                    return ['foofoo', 'bar', 'fun', 'bar', 'foo'][row]
                if colname in ['t1', 'n']:
                    return ['fun', 'bar', 'foo', 'barr', 'foooo'][row]
                elif colname == 's':
                    return [['foo', 'bar'], ['fun', 'bar'], ['foo']][row % 3]
                elif colname == 'n2':
                    return ['1', '12', '123', '1234', '12345'][row]
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
            if 'text' in suffix:
                for j, n in enumerate(textcolnames):
                    for i in range(5):
                        v = row_value(i, j, n, text=True)
                        columns[n].append(v)
            else:
                for j, n in enumerate(colnames):
                    for i in range(10 if '10' in suffix else 5):
                        v = row_value(i, j, n, null=('null' in suffix), arr=('array' in suffix))
                        columns[n].append(v)
            m.load_table_columnar(f'{table_name}{suffix}', **columns)

    else:
        # slow method using SQL query statements
        def row_value(row, col, colname, null=False, arr=False, text=False):
            if text:
                raise ValueError('use heavydb_fixture(..., load_table_columnar=True)')
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
