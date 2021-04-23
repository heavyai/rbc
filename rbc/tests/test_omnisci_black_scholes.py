import pytest
from numba import njit
from collections import defaultdict
import numpy as np
import math

rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
if available_version and available_version < (5, 4):
    reason = ('New-style UDTFs (with Column arguments) are available'
              ' for omniscidb 5.4 or newer, '
              'currently connected to omniscidb '
              + '.'.join(map(str, available_version)))
    available_version = ()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


def read_csv(filename):
    d = defaultdict(list)
    f = open(filename, 'r')
    s = f.readlines()
    header = s[0].strip().split(',')
    for col in s[1:]:
        col = col.strip().split(',')
        for h, c in zip(header, col):
            d[h].append(float(c))
    return d


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = 'rbc_test_black_scholes'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))

    sqltypes = ['DOUBLE', 'DOUBLE', 'DOUBLE', 'DOUBLE', 'DOUBLE', 'DOUBLE']
    colnames = ['S', 'X', 'T', 'sigma', 'r', 'oprice']
    table_defn = ',\n'.join('%s %s' % (n, t)
                            for t, n in zip(sqltypes, colnames))
    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name} ({table_defn});'
        .format(**locals()))

    df = read_csv('./rbc/tests/data_black_scholes.csv')

    rows = 10
    for i in range(rows):
        S = 100.0
        X = df['STRIKE'][i]
        T = df['TIME'][i] / 125
        r = 0.0
        sigma = df['VLTY'][i]
        oprice = df['OPRICE'][i]

        table_row = f'{S}, {X}, {T}, {sigma}, {r}, {oprice}'
        m.sql_execute(
            'INSERT INTO {table_name} VALUES ({table_row})'.format(**locals()))
    m.table_name = table_name
    yield m
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))


@njit
def cnd_numba(d):
    A1 = 0.31938153
    A2 = -0.356563782
    A3 = 1.781477937
    A4 = -1.821255978
    A5 = 1.330274429
    RSQRT2PI = 0.39894228040143267793994605993438
    K = 1.0 / (1.0 + 0.2316419 * math.fabs(d))
    ret_val = (RSQRT2PI * math.exp(-0.5 * d * d) *
               (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))))
    if d > 0:
        ret_val = 1.0 - ret_val
    return ret_val


def test_black_scholes_udf(omnisci):
    if omnisci.has_cuda and omnisci.version < (5, 5):
        pytest.skip('crashes CUDA enabled omniscidb server'
                    ' [issue 60]')

    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('double(double, double, double, double, double)')
    def black_scholes_UDF(S, X, T, r, sigma):
        d1 = (np.log(S/X) + (r + 0.5 * sigma**2)*T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)

        cndd1 = cnd_numba(d1)
        cndd2 = cnd_numba(d2)

        expRT = math.exp((-1. * r) * T)

        callResult = (S * cndd1 - X * expRT * cndd2)
        return callResult

    S = 100.0
    r = 0.0

    descr, result = omnisci.sql_execute(
        f'select OPRICE, black_scholes_UDF('
        f'cast({S} as DOUBLE), X, T, cast({r} as DOUBLE), sigma)'
        f' from {omnisci.table_name}')

    for _, (expected, out) in enumerate(result):
        assert math.isclose(expected, out, abs_tol=0.0001)


def test_black_scholes_udtf(omnisci):
    if omnisci.has_cuda and omnisci.version < (5, 5):
        pytest.skip('crashes CUDA enabled omniscidb server'
                    ' [issue 169]')

    omnisci.reset()
    # register an empty set of UDFs in order to avoid unregistering
    # UDFs created directly from LLVM IR strings when executing SQL
    # queries:
    omnisci.register()

    @omnisci('int32(Column<double>, Column<double>, Column<double>, Column<double>, Column<double>,'  # noqa: E501
             ' RowMultiplier, OutputColumn<double>)')
    def black_scholes_udtf(S, X, T, r, sigma, m, out):
        input_row_count = len(X)

        for i in range(input_row_count):
            d1 = (np.log(S[i]/X[i]) + (r[i] + 0.5 * sigma[i]**2)*T[i]) / (sigma[i] * np.sqrt(T[i]))  # noqa: E501
            out[i] = S[i] + X[i] + T[i] + r[i] + sigma[i]
            d2 = d1 - sigma[i] * np.sqrt(T[i])

            cndd1 = cnd_numba(d1)
            cndd2 = cnd_numba(d2)

            expRT = math.exp((-1. * r[i]) * T[i])

            out[i] = (S[i] * cndd1 - X[i] * expRT * cndd2)

        return m * input_row_count

    _, result = omnisci.sql_execute(
        'select * from table(black_scholes_udtf('
        ' cursor(select S from {omnisci.table_name}),'
        ' cursor(select X from {omnisci.table_name}),'
        ' cursor(select T from {omnisci.table_name}),'
        ' cursor(select r from {omnisci.table_name}),'
        ' cursor(select sigma from {omnisci.table_name}),'
        ' 1));'
        .format(**locals()))

    _, oprice = omnisci.sql_execute(f'select OPRICE from {omnisci.table_name}')

    for r, o in zip(result, oprice):
        assert math.isclose(o[0], r[0], abs_tol=0.0001)
