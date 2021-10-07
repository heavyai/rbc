import pytest
from rbc.tests import omnisci_fixture, sql_execute
import numpy as np


@pytest.fixture(scope='module')
def omnisci():
    for o in omnisci_fixture(globals()):
        define(o)
        yield o


def define(omnisci):

    T = ['int64', 'int32', 'int16', 'int8', 'float64', 'float32']

    if omnisci.version >= (5, 6):

        @omnisci('int32(Cursor<int64, T>, T, RowMultiplier,'
                 ' OutputColumn<int64>, OutputColumn<T>)', T=T)
        def sqlmultiply(rowid, col, alpha, row_multiplier, rowid_out, out):
            for i in range(len(col)):
                j = rowid[i]
                out[j] = col[i] * alpha
                rowid_out[j] = i
            return len(col)

        @omnisci('int32(Cursor<int64, T>, T, RowMultiplier,'
                 ' OutputColumn<int64>, OutputColumn<T>)', T=T)
        def sqladd(rowid, col, alpha, row_multiplier, rowid_out, out):
            for i in range(len(col)):
                j = rowid[i]
                out[j] = col[i] + alpha
                rowid_out[j] = i
            return len(col)

        @omnisci('int32(Cursor<int64, T>, Cursor<int64, T>, RowMultiplier,'
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

        @omnisci('int32(Cursor<int64, T>, Cursor<int64, T>, RowMultiplier,'
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
def test_composition(omnisci, kind):
    omnisci.require_version((5, 6), 'Requires omniscidb-internal PR 5440')

    def tonp(query):
        _, result = omnisci.sql_execute(query)
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

    select0 = f'select rowid, {kind} from {omnisci.table_name}'
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


def test_simple(omnisci):
    if omnisci.version >= (5, 4):
        reason = ('Old-style UDTFs are available for omniscidb 5.3 or older, '
                  'currently connected to omniscidb '
                  + '.'.join(map(str, omnisci.version)))
        pytest.skip(reason)

    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    def my_row_copier1(x, input_row_count_ptr, output_row_count, y):
        # sizer type is Constant
        m = 5
        input_row_count = input_row_count_ptr[0]
        n = m * input_row_count
        for i in range(input_row_count):
            for c in range(m):
                y[i + c * input_row_count] = x[i]
        output_row_count[0] = n
        return 0

    if 0:
        omnisci('int32|table(double*|cursor, int64*, int64*, double*|output)')(
            my_row_copier1)
        # Exception: Failed to allocate 5612303629517800 bytes of memory
        descr, result = omnisci.sql_execute(
            'select * from table(my_row_copier1(cursor(select f8 '
            'from {omnisci.table_name})));'
            .format(**locals()))

        for i, r in enumerate(result):
            print(i, r)

    def my_row_copier2(x,
                       n_ptr: dict(sizer='kUserSpecifiedConstantParameter'),  # noqa: F821,E501
                       input_row_count_ptr, output_row_count, y):
        n = n_ptr[0]
        m = 5
        input_row_count = input_row_count_ptr[0]
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                if j < n:
                    y[j] = x[i]
                else:
                    break
        output_row_count[0] = n
        return 0

    if 0:
        omnisci('int32|table(double*|cursor, int32*|input, int64*, int64*,'
                ' double*|output)')(my_row_copier2)
        # Exception: Failed to allocate 5612303562962920 bytes of memory
        descr, result = omnisci.sql_execute(
            'select * from table(my_row_copier2(cursor(select f8 '
            'from {omnisci.table_name}), 2));'
            .format(**locals()))

        for i, r in enumerate(result):
            print(i, r)

    @omnisci('double(double)')
    def myincr(x):
        return x + 1.0

    @omnisci('int32|table(double*|cursor, int32*|input, int64*, int64*,'
             ' double*|output)')
    def my_row_copier3(x,
                       m_ptr: dict(sizer='kUserSpecifiedRowMultiplier'),  # noqa: F821,E501
                       input_row_count_ptr, output_row_count, y):
        m = m_ptr[0]
        input_row_count = input_row_count_ptr[0]
        for i in range(input_row_count):
            for c in range(m):
                j = i + c * input_row_count
                y[j] = x[i] * 2
        output_row_count[0] = m * input_row_count
        return 0

    descr, result = omnisci.sql_execute(
        'select f8, myincr(f8) from table(my_row_copier3(cursor(select f8 '
        'from {omnisci.table_name}), 2));'
        .format(**locals()))

    for i, r in enumerate(result):
        assert r == ((i % 5) * 2, (i % 5) * 2 + 1)


@pytest.mark.parametrize("sleep", ['ct_sleep1', 'ct_sleep2'])
@pytest.mark.parametrize("mode", [0, 1, 2, 3, 4])
def test_parallel_execution(omnisci, sleep, mode):
    """This test is affected by omniscidb server option --num-executors.
    Here we start a number of identical tasks at the same time and
    observe if the tasks are run in-parallel (ct_sleep2) or not
    (ct_sleep1).

    """
    omnisci.require_version((5, 8), 'Requires omniscidb-internal PR 5901',
                            label='qe-99')
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
    # such tests and rerunning pytest many times may lead to omniscidb
    # server crash if num-executors>1. This may indicate that
    # omniscidb error handling is not completely thread-save.
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
