import pytest
from rbc.tests import omnisci_fixture
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
