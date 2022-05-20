import pytest
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), minimal_version=(6, 1), load_test_data=False):
        yield o


def test_get_registered_udtfs(heavydb):
    assert len(heavydb.table_function_names()) > 0


def test_get_runtime_udtfs_empty(heavydb):
    heavydb.unregister()
    assert len(heavydb.table_function_names(runtime_only=True)) == 0


def test_get_runtime_udtfs_non_empty(heavydb):
    heavydb.unregister()

    @heavydb('int32(Column<int>, OutputColumn<int>)')
    def foo(x, y):
        return 0

    heavydb.register()
    udtfs = heavydb.table_function_names(runtime_only=True)
    assert len(udtfs) == 1
    assert udtfs[0] == 'foo'


def test_get_udtf_details(heavydb):
    heavydb.unregister()

    @heavydb('int32(Column<int>, OutputColumn<int>)')
    def my_udtf(x, y):
        return 0

    heavydb.register()
    details = heavydb.table_function_details('my_udtf')

    @heavydb('int32(Column<int>, ConstantParameter, OutputColumn<int>)')
    def another_udtf(x, m, y):
        return 0

    heavydb.register()
    details = heavydb.table_function_details('my_udtf', 'another_udtf')
    assert len(details) == 2


def test_get_invalid_udtf_details(heavydb):
    heavydb.reset()

    details = heavydb.table_function_details('invalid_udtf1234')
    assert len(details) == 0


def test_get_udfs_registered(heavydb):
    assert len(heavydb.function_names()) > 0


def test_get_runtime_udfs_empty(heavydb):
    heavydb.reset()
    assert len(heavydb.function_names(runtime_only=True)) == 0


def test_get_udf_details(heavydb):
    heavydb.reset()

    @heavydb('int32(int32)')
    def incr(a):
        return a + 1

    heavydb.register()

    assert len(heavydb.function_details('incr')) == 1


def get_invalid_udf_details(heavydb):
    heavydb.reset()

    assert len(heavydb.function_details('invalid_udf_12345')) == 0
