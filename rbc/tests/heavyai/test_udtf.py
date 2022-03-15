import pytest
from rbc.errors import NumbaTypeError
from rbc.tests import heavydb_fixture, sql_execute
from rbc.externals.heavydb import table_function_error
import numpy as np


@pytest.fixture(scope='module')
def heavydb():
    for o in heavydb_fixture(globals()):
        define(o)
        yield o


def define(heavydb):

    T = ['int64', 'int32', 'int16', 'int8', 'float64', 'float32']

    @heavydb('int32(Cursor<int64, T>, T, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<T>)', T=T)
    def sqlmultiply(rowid, col, alpha, row_multiplier, rowid_out, out):
        for i in range(len(col)):
            j = rowid[i]
            out[j] = col[i] * alpha
            rowid_out[j] = i
        return len(col)

    @heavydb('int32(Cursor<int64, T>, T, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<T>)', T=T)
    def sqladd(rowid, col, alpha, row_multiplier, rowid_out, out):
        for i in range(len(col)):
            j = rowid[i]
            out[j] = col[i] + alpha
            rowid_out[j] = i
        return len(col)

    @heavydb('int32(Cursor<int64, T>, Cursor<int64, T>, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<T>)', T=T)
    def sqladd2(rowid1, col1, rowid2, col2, row_multiplier, rowid_out, out):
        for i1 in range(len(col1)):
            j1 = rowid1[i1]
            for i2 in range(len(col2)):
                j2 = rowid2[i2]
                if j1 == j2:
                    out[j1] = col1[i1] + col2[i2]
                    rowid_out[j1] = i1
                    break
        return len(col1)

    @heavydb('int32(Cursor<int64, T>, Cursor<int64, T>, RowMultiplier,'
             ' OutputColumn<int64>, OutputColumn<T>)', T=T)
    def sqlmultiply2(rowid1, col1, rowid2, col2, row_multiplier, rowid_out, out):
        for i1 in range(len(col1)):
            j1 = rowid1[i1]
            for i2 in range(len(col2)):
                j2 = rowid2[i2]
                if j1 == j2:
                    out[j1] = col1[i1] * col2[i2]
                    rowid_out[j1] = i1
                    break
        return len(col1)


@pytest.mark.parametrize("kind", ['i8', 'i4', 'i2', 'i1', 'f8', 'f4'])
def test_composition(heavydb, kind):
    heavydb.require_version((5, 6), 'Requires heavydb-internal PR 5440')

    def tonp(query):
        _, result = heavydb.sql_execute(query)
        return tuple(np.array(a) for a in zip(*list(result)))

    sqltyp = dict(i8='BIGINT', i4='INT', i2='SMALLINT', i1='TINYINT',
                  f8='DOUBLE', f4='FLOAT')[kind]

    class Algebra:

        def __init__(self, arr, select):
            self.arr = arr
            self.select = select

        def __add__(self, other):
            if isinstance(other, (int, float)):
                return type(self)(
                    self.arr + other,
                    'select out0, out1 from table(sqladd(cursor('
                    f'{self.select}), cast({other} as {sqltyp}), 1))')
            elif isinstance(other, type(self)):
                return type(self)(
                    self.arr + other.arr,
                    'select out0, out1 from table(sqladd2(cursor('
                    f'{self.select}), cursor({other.select}), 1))')
            return NotImplemented

        def __mul__(self, other):
            if isinstance(other, (int, float)):
                return type(self)(
                    self.arr * other,
                    'select out0, out1 from table(sqlmultiply(cursor('
                    f'{self.select}), cast({other} as {sqltyp}), 1))')
            elif isinstance(other, type(self)):
                return type(self)(
                    self.arr * other.arr,
                    'select out0, out1 from table(sqlmultiply2(cursor('
                    f'{self.select}), cursor({other.select}), 1))')
            return NotImplemented

        def isok(self):
            return (tonp(self.select)[1] == self.arr).all()

        def __repr__(self):
            return f'{type(self).__name__}({self.arr!r}, {self.select!r})'

    select0 = f'select rowid, {kind} from {heavydb.table_name}'
    arr0 = tonp(select0)[1]

    a = Algebra(arr0, select0)

    assert a.isok()

    assert (a + 2).isok()
    assert ((a + 2) + 3).isok()

    assert (a * 2).isok()
    assert ((a * 2) + 3).isok()

    assert (a + a).isok()
    assert (a * a).isok()

    assert ((a + 2) * a).isok()
    assert ((a + 2) * a + 3).isok()


def test_table_function_manager(heavydb):
    heavydb.require_version((5, 9), 'Requires heavydb-internal PR 6035')

    @heavydb('int32(TableFunctionManager, Column<double>, OutputColumn<double>)')
    def my_manager_error(mgr, col, out):
        return mgr.error_message("TableFunctionManager error_message!")

    @heavydb('int32(TableFunctionManager, Column<double>, OutputColumn<double>)')
    def my_manager_row_size(mgr, col, out):
        size = len(col)
        mgr.set_output_row_size(size)
        for i in range(size):
            out[i] = col[i]
        return size

    with pytest.raises(heavydb.thrift_client.thrift.TMapDException) as exc:
        heavydb.sql_execute(
            f'select out0 from table(my_manager_error('
            f'cursor(select f8 from {heavydb.table_name})));')

    assert exc.match('Error executing table function: TableFunctionManager error_message!')

    _, result = heavydb.sql_execute(
        f'select out0 from table(my_manager_row_size('
        f'cursor(select f8 from {heavydb.table_name})));')

    expected = [(0.0,), (1.0,), (2.0,), (3.0,), (4.0,)]
    assert(list(result) == expected)


@pytest.mark.parametrize("sleep", ['ct_sleep1', 'ct_sleep2'])
@pytest.mark.parametrize("mode", [0, 1, 2, 3, 4])
def test_parallel_execution(heavydb, sleep, mode):
    """This test is affected by heavydb server option --num-executors.
    Here we start a number of identical tasks at the same time and
    observe if the tasks are run in-parallel (ct_sleep2) or not
    (ct_sleep1).

    """
    heavydb.require_version((5, 9), 'Requires heavydb-internal PR 5901')
    from multiprocessing import Process, Array

    def func(seconds, mode, a):
        try:
            descr, result = sql_execute(
                f'select * from table({sleep}({seconds}, {mode}));')
        except Exception as msg:
            code = -1  # error code, positive if exception is expected
            if ((mode == 2 or (mode == 3 and sleep == 'ct_sleep1')
                 and 'unspecified output columns row size' in str(msg))):
                code = 2
            elif mode == 3 and 'uninitialized TableFunctionManager singleton' in str(msg):
                code = 3
            elif mode == 4 and 'unexpected mode' in str(msg):
                code = 4
            else:
                print(msg)
            a[0] = -1    # indicates exception
            a[1] = code
            return
        for i, v in enumerate(result):
            a[i] = v[0]

    # mode > 1 correspond to exception tests. Increasing nof_jobs for
    # such tests and rerunning pytest many times may lead to heavydb
    # server crash if num-executors>1. This may indicate that
    # heavydb error handling is not completely thread-save.
    nof_jobs = 5 if mode <= 1 else 1
    sleep_seconds = 1

    # Initialize tasks
    processes_outputs = []
    for i in range(nof_jobs):
        a = Array('i', [0, 0, 0])
        p = Process(target=func, args=(sleep_seconds, mode, a))
        processes_outputs.append((p, a))

    # Start tasks
    for p, _ in processes_outputs:
        p.start()

    # Collect outputs
    outputs = []
    for p, a in processes_outputs:
        p.join()
        if a[0] == -1:
            if a[1] < 0:
                raise ValueError(f'unexpected failure: code={a[1]}')
            continue
        outputs.append(a[:])

    if outputs:
        outputs = sorted(outputs)
        origin = outputs[0][0]  # in milliseconds
        print('\nthread id:---> time, 1 character == 100 ms, `*` marks an UDTF execution')
        for start, end, thread_id in outputs:
            print(f'{thread_id:8x} :' + ' ' * ((start - origin) // 100)
                  + '*' * ((end - start) // 100))


def test_table_function_error(heavydb):
    heavydb.require_version((5, 8), 'Requires heavydb-internal PR 5879')
    heavydb.reset()

    @heavydb('int32(Column<double>, double, RowMultiplier, OutputColumn<double>)')
    def my_divide(column, k, row_multiplier, out):
        if k == 0:
            return table_function_error('division by zero')
        for i in range(len(column)):
            out[i] = column[i] / k
        return len(column)

    descr, result = heavydb.sql_execute(f"""
        select *
        from table(
            my_divide(CURSOR(SELECT f8 FROM {heavydb.table_name}), 2)
        );
    """)
    assert list(result) == [(0.0,), (0.5,), (1.0,), (1.5,), (2.0,)]

    with pytest.raises(heavydb.thrift_client.thrift.TMapDException) as exc:
        heavydb.sql_execute(f"""
            select *
            from table(
                my_divide(CURSOR(SELECT f8 FROM {heavydb.table_name}), 0)
            );
        """)
    assert exc.match('Error executing table function: division by zero')


def test_raise_error(heavydb):
    heavydb.require_version((5, 8), 'Requires heavydb-internal PR 5879')
    heavydb.reset()

    with pytest.raises(NumbaTypeError) as exc:
        @heavydb('int32(Column<double>, double, RowMultiplier, OutputColumn<double>)')
        def my_divide(column, k, row_multiplier, out):
            if k == 0:
                raise ValueError('division by zero')
            for i in range(len(column)):
                out[i] = column[i] / k
            return len(column)

        heavydb.register()

    assert exc.match('raise statement is not supported')
    heavydb.reset()


def test_issue_235(heavydb):

    @heavydb('int32(Column<int64>, RowMultiplier, OutputColumn<int64>)')
    def text_rbc_copy_rowmul(x, m, y):
        for i in range(len(x)):
            y[i] = x[i]
        return len(x)

    @heavydb('int32(Cursor<int64, double>, RowMultiplier, OutputColumn<int64>, OutputColumn<double>)')  # noqa: E501
    def text_rbc_copy_rowmul(x, x2, m, y, y2):  # noqa: F811
        for i in range(len(x)):
            y[i] = x[i]
            y2[i] = x2[i]
        return len(x)

    query = (f'select * from table(text_rbc_copy_rowmul('
             f'cursor(select i8, f8 from {heavydb.table_name}), 1));')
    _, result = heavydb.sql_execute(query)
    assert list(result) == list(zip(np.arange(5), np.arange(5, dtype=np.float64)))
