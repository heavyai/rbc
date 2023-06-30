# tests for how-tos page
# ensure that examples in the docs page are correct
import pytest
from rbc.errors import HeavyDBServerError
from rbc.tests import heavydb_fixture


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals()):
        yield o


def test_connect(heavydb):
    # magictoken.connect.begin
    from rbc.heavydb import RemoteHeavyDB
    heavy = RemoteHeavyDB(user='admin', password='HyperInteractive',
                          host='127.0.0.1', port=6274)
    # magictoken.connect.end
    assert heavy.version is not None


def test_external_functions(heavydb):
    # magictoken.external_functions.abs.begin
    from rbc.external import external
    cmath_abs = external('int64 abs(int64)')

    @heavydb('int64(int64)')
    def apply_abs(x):
        return cmath_abs(x)

    _, result = heavydb.sql_execute('SELECT apply_abs(-3);')
    # magictoken.external_functions.abs.end
    assert list(result) == [(3,)]

    # magictoken.external_functions.printf.begin
    from rbc.externals.stdio import printf

    @heavydb('int64(int64)')
    def power_2(x):
        # This message will show in the heavydb logs
        printf("input number: %d\n", x)
        return x * x

    _, result = heavydb.sql_execute('SELECT power_2(3);')
    # magictoken.external_functions.printf.end
    assert list(result) == [(9,)]


def test_raise_exception(heavydb):
    # magictoken.raise_exception.begin
    @heavydb('int32(TableFunctionManager, Column<int>, OutputColumn<int>)')
    def udtf_copy(mgr, inp, out):
        size = len(inp)
        if size > 4:
            # error message must be known at compile-time
            return mgr.error_message('TableFunctionManager error_message!')

        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = inp[i]
        return size
    # magictoken.raise_exception.end

    col = 'i4'
    table_name = heavydb.table_name

    query = f'''
        SELECT
          *
        FROM
            TABLE(udtf_copy(
                cursor(SELECT {col} FROM {table_name})
            ))
    '''

    with pytest.raises(HeavyDBServerError) as exc:
        heavydb.sql_execute(query)
    exc_msg = ('Error executing table function: TableFunctionManager '
               'error_message!')
    assert exc.match(exc_msg)


def test_devices(heavydb):
    # magictoken.devices.begin
    @heavydb('int32(int32, int32)', devices=['CPU', 'GPU'])
    def add(a, b):
        return a + b
    # magictoken.devices.end
    _, result = heavydb.sql_execute('SELECT add(-3, 3);')
    assert list(result) == [(0,)]


def test_templates(heavydb):
    heavydb.unregister()

    # magictoken.templates.begin
    @heavydb('Z(T, Z)', T=['int32', 'float'], Z=['int64', 'double'])
    def add(a, b):
        return a + b

    heavydb.register()
    assert heavydb.function_names(runtime_only=True) == ['add']
    assert len(heavydb.function_details('add')) == 4
    # magictoken.templates.end
