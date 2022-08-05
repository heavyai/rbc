__all__ = ['heavydb_fixture', 'sql_execute']


from abc import abstractclassmethod, abstractproperty
import numpy as np
import os
import pytest
import warnings
import numpy

from packaging.version import Version


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


class _TestTable:

    @abstractclassmethod
    def suffix(cls):
        pass

    @abstractproperty
    def sqltypes(self):
        pass

    @property
    def colnames(self):
        return self.values.keys()

    @abstractproperty
    def values(self):
        pass

    @property
    def table_defn(self):
        return ',\n'.join(f'{n} {t}' for n, t in zip(self.colnames, self.sqltypes))


class _DefaultTestTable(_TestTable):

    @classmethod
    def suffix(cls):
        return ''

    @property
    def sqltypes(self):
        return ('FLOAT', 'DOUBLE', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                'BOOLEAN')

    @property
    def values(self):
        return {
            'f4': [0, 1, 2, 3, 4],
            'f8': [0, 1, 2, 3, 4],
            'i1': [0, 1, 2, 3, 4],
            'i2': [0, 1, 2, 3, 4],
            'i4': [0, 1, 2, 3, 4],
            'i8': [0, 1, 2, 3, 4],
            'b': [True, False, True, False, True],
        }


class _10TestTable(_DefaultTestTable):

    @classmethod
    def suffix(cls):
        return '10'

    @property
    def values(self):
        return {
            'f4': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'f8': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'i1': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'i2': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'i4': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'i8': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'b': [True, False, True, False, True, False, True, False, True, False],
        }


class _nullTestTable(_DefaultTestTable):

    @classmethod
    def suffix(cls):
        return "null"

    @property
    def values(self):
        return {
            'f4': [None, 1, 2, None, 4],
            'f8': [0, 1, None, 3, 4],
            'i1': [0, None, 2, 3, None],
            'i2': [None, 1, 2, None, 4],
            'i4': [0, 1, None, 3, 4],
            'i8': [0, None, 2, 3, None],
            'b': [None, False, True, None, True],
        }


class _arrayTestTable(_DefaultTestTable):

    @classmethod
    def suffix(cls):
        return "array"

    @property
    def sqltypes(self):
        return ('FLOAT[]', 'DOUBLE[]', 'TINYINT[]', 'SMALLINT[]', 'INT[]', 'BIGINT[]',
                'BOOLEAN[]')

    @property
    def values(self):
        return {
            'f4': [[], [1], [2, 3], [3, 4, 5], [4, 5, 6, 7]],
            'f8': [[], [1], [2, 3], [3, 4, 5], [4, 5, 6, 7]],
            'i1': [[], [1], [2, 3], [3, 4, 5], [4, 5, 6, 7]],
            'i2': [[], [1], [2, 3], [3, 4, 5], [4, 5, 6, 7]],
            'i4': [[], [1], [2, 3], [3, 4, 5], [4, 5, 6, 7]],
            'i8': [[], [1], [2, 3], [3, 4, 5], [4, 5, 6, 7]],
            'b': [[], [False], [True, False], [False, True, False], [True, False, True, False]],
        }


class _arraynullTestTable(_arrayTestTable):

    @classmethod
    def suffix(cls):
        return "arraynull"

    @property
    def values(self):
        return {
            'f4': [None, [1], None, [None, 4, 5], None],
            'f8': [[], None, [None, 3], None, [4, None, 6, 7]],
            'i1': [None, [None], None, [3, None, 5], None],
            'i2': [[], None, [2, None], None, [4, 5, None, 7]],
            'i4': [None, [1], None, [3, 4, None], None],
            'i8': [[], None, [2, 3], None, [None, 5, 6, None]],
            'b': [None, [False], None, [None, True, False], None],
        }


class _TimestampTestTable(_TestTable):

    @classmethod
    def suffix(cls):
        return 'timestamp'

    @property
    def sqltypes(self):
        return ('TIMESTAMP(9)', 'TIMESTAMP(9)', 'TIMESTAMP(9)', 'BIGINT', 'TIMESTAMP(6)')

    @property
    def values(self):
        return {
            "t9": [np.datetime64("1971-01-01 01:01:01.001001001").astype('long'),
                   np.datetime64("1972-02-02 02:02:02.002002002").astype('long'),
                   np.datetime64("1973-03-03 03:03:03.003003003").astype('long')],
            "t9_2": [np.datetime64("2021-01-01 01:01:01.001001001").astype('long'),
                     np.datetime64("2022-02-02 02:02:02.002002002").astype('long'),
                     np.datetime64("2023-03-03 03:03:03.003003003").astype('long')],
            "t9_null": [np.datetime64("1972-02-02 02:02:02.002002002").astype('long'),
                        np.datetime64('NaT').astype('long'),
                        np.datetime64("2037-02-02 02:02:02.002002002").astype('long')],
            "i8_2": [1609462861001001001, 1643767322002002002, 1677812583003003003],
            't6': [np.datetime64("1971-01-01 01:01:01.001001").astype('long'),
                   np.datetime64("1972-02-02 02:02:02.002002").astype('long'),
                   np.datetime64("1973-03-03 03:03:03.003003").astype('long')]
        }


class _TextTestTable(_TestTable):

    @classmethod
    def suffix(cls):
        return 'text'

    @property
    def sqltypes(self):
        return ('TEXT ENCODING DICT(32)', 'TEXT ENCODING DICT(16)',
                'TEXT ENCODING DICT(8)', 'TEXT[] ENCODING DICT(32)',
                'TEXT ENCODING NONE', 'TEXT ENCODING NONE')

    @property
    def colnames(self):
        return ('t4', 't2', 't1', 's', 'n', 'n2')

    @property
    def values(self):
        return {
            't4': ['foofoo', 'bar', 'fun', 'bar', 'foo'],
            't2': ['foofoo', 'bar', 'fun', 'bar', 'foo'],
            't1': ['fun', 'bar', 'foo', 'barr', 'foooo'],
            's': [['foo', 'bar'], ['fun', 'bar'], ['foo'], ['foo', 'bar'], ['fun', 'bar']],
            'n': ['fun', 'bar', 'foo', 'barr', 'foooo'],
            'n2': ['1', '12', '123', '1234', '12345'],
        }


def heavydb_fixture(caller_globals, minimal_version=(0, 0),
                    suffices=['', '10', 'null', 'array', 'arraynull', 'text', 'timestamp'],
                    load_test_data=True, debug=False):
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

    f'{heavydb.table_name}timestamp' - contains timestamp t9, t6
                                  where 't' prefix is for timestamp.
    """
    rbc_heavydb = pytest.importorskip('rbc.heavydb')
    available_version, reason = rbc_heavydb.is_available()

    def skip_on_docker():
        curr_version = Version('.'.join(map(str, available_version[:2])))
        if os.environ.get('CI') and os.environ.get('HEAVYDB_DOCKER_IMAGE') and \
                curr_version >= (6, 1):
            pytest.skip('HeavyDB docker is not built with -DENABLE_SYSTEM_TFS')

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
        current_development_version = Version("6.2.0")

        curr_version = Version('.'.join(map(str, available_version[:2])))

        if curr_version > current_development_version:
            warnings.warn(f'{curr_version}) is newer than development version'
                          f' ({current_development_version}), please update the latter!')

        assert isinstance(version, tuple)
        version = Version('.'.join(map(str, version)))
        if version > curr_version:
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
                           f' got {curr_version} with label {env_label}')
                if message is not None:
                    _reason += f': {message}'
                pytest.skip(_reason)

            if version < curr_version:
                # in the case the branch given in the label was never
                # merged, consider removing the corresponding test
                warnings.warn(f'detected test requiring {version} with out-of-date label {label}.'
                              ' Please reset test label to None.')

    require_version(minimal_version)

    filename = caller_globals['__file__']
    table_name = os.path.splitext(os.path.basename(filename))[0]

    config = rbc_heavydb.get_client_config(debug=debug)
    m = rbc_heavydb.RemoteHeavyDB(**config)
    m.require_version = require_version
    m.skip_on_docker = skip_on_docker

    if not load_test_data:
        yield m
        return

    # todo: TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://docs.heavy.ai/sql/data-definition-ddl/datatypes-and-fixed-encoding
    for cls in (_DefaultTestTable, _10TestTable, _nullTestTable, _arrayTestTable,
                _arraynullTestTable, _TextTestTable, _TimestampTestTable):
        suffix = cls.suffix()
        if suffix in suffices:
            obj = cls()
            m.sql_execute(f'DROP TABLE IF EXISTS {table_name}{suffix}')
            m.sql_execute(f'CREATE TABLE IF NOT EXISTS {table_name}{suffix} ({obj.table_defn})')
            m.load_table_columnar(f'{table_name}{suffix}', **obj.values)

    m.table_name = table_name
    yield m
    for suffix in suffices:
        m.sql_execute(f'DROP TABLE IF EXISTS {table_name}{suffix}')
