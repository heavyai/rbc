import os
import pytest

rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = os.path.splitext(os.path.basename(__file__))[0]
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    sqltypes = ['FLOAT[]', 'DOUBLE[]',
                'TINYINT[]', 'SMALLINT[]', 'INT[]', 'BIGINT[]',
                'BOOLEAN[]']
    # todo: TEXT ENCODING DICT, TEXT ENCODING NONE, TIMESTAMP, TIME,
    # DATE, DECIMAL/NUMERIC, GEOMETRY: POINT, LINESTRING, POLYGON,
    # MULTIPOLYGON, See
    # https://www.omnisci.com/docs/latest/5_datatypes.html
    colnames = ['f4', 'f8', 'i1', 'i2', 'i4', 'i8', 'b']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});'
        .format(**locals()))

    m.sql_execute(f'insert into {table_name} values '
                  '(NULL, NULL, NULL, NULL, NULL, NULL, NULL);')
    m.sql_execute(f"insert into {table_name} values ("
                  "{NULL, 2.0}, {NULL, 3.0}, {NULL, 1}, {NULL, 2},"
                  "{NULL, 3}, {NULL, 4}, {NULL, 'true'});")

    m.table_name = table_name
    yield m
    try:
        m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))
    except Exception as msg:
        print('%s in deardown' % (type(msg)))


@pytest.mark.parametrize('col', ['i1', 'i2', 'i4', 'i8', 'f4', 'f8'])
def test_array_isnull_issue240(omnisci, col):
    omnisci.reset()

    @omnisci('T(T[], int64, int64)', T=['int8', 'int16', 'int32', 'int64', 'float', 'double'])
    def array_null_check(x, i, value):
        if x.is_null():  # array row is null
            return value
        if x.is_null(i):  # array element is null
            return value
        return x[i]

    _, result = omnisci.sql_execute(
        f'SELECT {col}, array_null_check({col}, 0, 321) FROM {omnisci.table_name};')

    print(list(result))
