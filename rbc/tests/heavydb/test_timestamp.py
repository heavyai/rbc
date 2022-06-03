from rbc.heavydb import Timestamp
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
    heavydb.sql_execute("insert into time_test values ('1971-01-01 01:01:01.001001001');")
    heavydb.sql_execute("insert into time_test values ('1972-02-02 02:02:02.002002002');")
    heavydb.sql_execute("insert into time_test values ('1973-03-03 03:03:03.003003003');")
    heavydb.sql_execute("insert into time_test values ('2022-06-02 15:00:00.000000000');")
    # @heavydb("int32_t(Column<Timestamp>, OutputColumn<int64_t>)")
    # def timestamp_extract(x, y):
    #     set_output_row_size(len(x))
    #     for i in range(len(x)):
    #         y[i] = x[i]
    #     return len(x)

    # @heavydb("int32_t(Column<Timestamp>, OutputColumn<int64_t>)")
    # def timestamp_extract_year(x, y):
    #     set_output_row_size(len(x))
    #     for i in range(len(x)):
    #         y[i] = x[i].getYear()
    #     return len(x)

    # @heavydb('int64(Timestamp)')
    # def foo(t):
    #     return t.getYear()

    @heavydb('Timestamp(int64, int64)')
    def bar(t1, t2):
        return Timestamp(t1 + t2)

    heavydb.register()
    # print(foo.describe())
    # table = 'time_test'
    # query = f'select * from table(timestamp_extract_year(cursor(select t1 from {table})));'
    # _, result = heavydb.sql_execute(query)
    # print(list(result))


