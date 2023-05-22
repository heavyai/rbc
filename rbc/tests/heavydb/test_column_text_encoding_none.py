import numpy as np
import pytest

from rbc.tests import heavydb_fixture

rbc_heavydb = pytest.importorskip('rbc.heavydb')
available_version, reason = rbc_heavydb.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals(), suffices=['text']):
        define(o)
        yield o


def define(heavydb):

    @heavydb('int32(TableFunctionManager, Column<T>, OutputColumn<T>)',
             T=['TextEncodingNone'])
    def rbc_ct_copy(mgr, inputs, outputs):
        size = len(inputs)
        mgr.set_output_item_values_total_number(0, inputs.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if inputs.is_null(i):
                outputs.set_null(i)
            else:
                outputs[i] = inputs[i]
        return size

    @heavydb('int32(TableFunctionManager, Cursor<Column<T>, Column<T>>, OutputColumn<T>)',
             T=['TextEncodingNone'], devices=['CPU'])
    def rbc_ct_concat__generic1(mgr, input1, input2, outputs):
        size = len(input1)
        mgr.set_output_item_values_total_number(
            0, input1.get_n_of_values() + input2.get_n_of_values())
        mgr.set_output_row_size(size)
        for i in range(size):
            if input1.is_null(i):
                if input2.is_null(i):
                    outputs.set_null(i)
                else:
                    outputs[i] = input2[i]
            elif input2.is_null(i):
                outputs[i] = input1[i]
            else:
                outputs[i] = input1[i]
                outputs.concat_item(i, input2[i])
        return size

    @heavydb('int32(TableFunctionManager, Column<T>, T, OutputColumn<T>)',
             T=['TextEncodingNone'], devices=['CPU'])
    def rbc_ct_concat__generic2(mgr, input1, input2, output):
        size = len(input1)
        mgr.set_output_item_values_total_number(
            0, input1.get_n_of_values() + size * len(input2))
        mgr.set_output_row_size(size)
        for i in range(size):
            if input1.is_null(i) or input2.is_null():
                output.set_null(i)
            else:
                output[i] = input1[i]
                output.concat_item(i, input2)
        return size

    @heavydb('int32(TableFunctionManager, T, Column<T>, OutputColumn<T>)',
             T=['TextEncodingNone'], devices=['CPU'])
    def rbc_ct_concat__generic3(mgr, input1, input2, output):
        size = len(input2)
        mgr.set_output_item_values_total_number(
            0, input2.get_n_of_values() + size * len(input1))
        mgr.set_output_row_size(size)
        for i in range(size):
            if input2.is_null(i) or input1.is_null():
                output.set_null(i)
            else:
                output[i] = input1
                output.concat_item(i, input2[i])
        return size


@pytest.mark.parametrize('col', ('n', 'n2'))
def test_rbc_ct_copy(heavydb, col):
    if heavydb.version[:2] < (7, 0):
        pytest.skip('Requires HeavyDB 7.0 or newer')

    suffix = 'text'
    table = heavydb.table_name + suffix

    query = (f"SELECT * FROM TABLE(rbc_ct_copy(CURSOR(SELECT "
             f"{col} from {table})));")
    _, result = heavydb.sql_execute(query)

    query = (f"SELECT * FROM TABLE(ct_copy(CURSOR(SELECT "
             f"{col} from {table})));")
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('col', ('n', 'n2'))
def test_rbc_ct_concat(heavydb, col):
    if heavydb.version[:2] < (7, 0):
        pytest.skip('Requires HeavyDB 7.0 or newer')

    suffix = 'text'
    table = heavydb.table_name + suffix

    query = (f"SELECT * FROM TABLE(rbc_ct_concat(CURSOR(SELECT "
             f"{col}, {col} from {table})));")
    _, result = heavydb.sql_execute(query)

    query = (f"SELECT * FROM TABLE(ct_concat(CURSOR(SELECT "
             f"{col}, {col} from {table})));")
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('col', ('n', 'n2'))
def test_rbc_ct_concat2(heavydb, col):
    if heavydb.version[:2] < (7, 0):
        pytest.skip('Requires HeavyDB 7.0 or newer')

    suffix = 'text'
    var = 'test'
    table = heavydb.table_name + suffix

    query = (f"SELECT * FROM TABLE(rbc_ct_concat(CURSOR(SELECT "
             f"{col} from {table}), '{var}'));")
    _, result = heavydb.sql_execute(query)

    query = (f"SELECT * FROM TABLE(ct_concat(CURSOR(SELECT "
             f"{col} from {table}), '{var}'));")
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)


@pytest.mark.parametrize('col', ('n', 'n2'))
def test_rbc_ct_concat3(heavydb, col):
    if heavydb.version[:2] < (7, 0):
        pytest.skip('Requires HeavyDB 7.0 or newer')

    suffix = 'text'
    var = 'test'
    table = heavydb.table_name + suffix

    query = (f"SELECT * FROM TABLE(rbc_ct_concat('{var}', "
             f"CURSOR(SELECT {col} from {table})));")
    _, result = heavydb.sql_execute(query)

    query = (f"SELECT * FROM TABLE(ct_concat('{var}', "
             f"CURSOR(SELECT {col} from {table})));")
    _, expected = heavydb.sql_execute(query)

    result = list(result)
    expected = list(expected)
    np.testing.assert_equal(result, expected)
