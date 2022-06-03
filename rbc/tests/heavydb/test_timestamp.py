from rbc.tests import heavydb_fixture
import pytest

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), debug=not True, load_columnar=True):
        yield o


def test_timestamp(heavydb):
    from rbc.externals.heavydb import set_output_row_size

    heavydb.sql_execute('drop table if exists time_test;')
    heavydb.sql_execute('create table time_test(t1 TIMESTAMP(9));')
    heavydb.sql_execute("insert into time_test values ('1970-01-01 00:00:00.000000001');")
    heavydb.sql_execute("insert into time_test values ('2022-06-03 15:00:00.000000000');")

    @heavydb("int32_t(Column<Timestamp>, OutputColumn<int64_t>)")
    def timestamp_extract(x, y):
        set_output_row_size(len(x))
        for i in range(len(x)):
            y[i] = x[i]
        return len(x)

    heavydb.register()

    table = 'time_test'
    query = f'select * from table(timestamp_extract(cursor(select t1 from {table})));'
    _, result = heavydb.sql_execute(query)
    assert list(result) == [1, 1654268400000]
