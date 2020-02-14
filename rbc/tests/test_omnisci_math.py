import numpy as np
import pytest
rbc_omnisci = pytest.importorskip('rbc.omniscidb')


def omnisci_is_available():
    """Return True if OmniSci server is accessible.
    """
    config = rbc_omnisci.get_client_config()
    omnisci = rbc_omnisci.RemoteOmnisci(**config)
    client = omnisci.client
    try:
        version = client(
                Omnisci=dict(get_version=()))['Omnisci']['get_version']
    except Exception as msg:
        return False, 'failed to get OmniSci version: %s' % (msg)
    if version >= '4.6':
        return True, None
    return False, 'expected OmniSci version 4.6 or greater, got %s' % (version)


is_available, reason = omnisci_is_available()
pytestmark = pytest.mark.skipif(not is_available, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = 'rbc_test_omnisci_math'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))

    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name}'
        ' (x DOUBLE, y DOUBLE, i INT, j INT, t INT[]);'
        .format(**locals()))

    for _i in range(1, 6):
        x = _i/10.0
        y = _i/6.0
        i = _i
        j = i * 10
        t = 'ARRAY[%s]' % (', '.join(str(j + i) for i in range(-i, i+1)))
        m.sql_execute(
            'insert into {table_name} values ({x}, {y}, {i}, {j}, {t})'
            .format(**locals()))

    m.table_name = table_name
    yield m

    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))


def test_trigonometric_funcs(omnisci):
    omnisci.reset()

    @omnisci('double(double)')  # noqa: F811
    def sin(x):
        return np.sin(x)

    @omnisci('double(double)')  # noqa: F811
    def cos(x):
        return np.cos(x)

    @omnisci('double(double)')  # noqa: F811
    def tan(x):
        return np.tan(x)

    @omnisci('double(double)')  # noqa: F811
    def arcsin(x):
        return np.arcsin(x)

    @omnisci('double(double)')  # noqa: F811
    def arccos(x):
        return np.arccos(x)

    @omnisci('double(double)')  # noqa: F811
    def arctan(x):
        return np.arctan(x)

    @omnisci('double(double, double)')  # noqa: F811
    def _hypot(x, y):
        return np.hypot(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def arctan2(x, y):
        return np.arctan2(x, y)

    omnisci.register()

    for fn_name in ['sin', 'cos', 'tan', 'arcsin',
                    'arccos', 'arctan', '_hypot', 'arctan2']:
        np_fn = getattr(np, fn_name.lstrip('_'))

        if fn_name in ['_hypot', 'arctan2']:
            descr, result = omnisci.sql_execute(
                'select x, y, {fn_name}(x, y) from {omnisci.table_name}'
                .format(**locals()))

            for x, y, v in list(result):
                assert(np.isclose(np_fn(x, y), v))
        else:
            descr, result = omnisci.sql_execute(
                'select x, {fn_name}(x) from {omnisci.table_name}'
                .format(**locals()))

            for x, v in list(result):
                assert(np.isclose(np_fn(x), v))


def test_hyperbolic_funcs(omnisci):
    omnisci.reset()

    @omnisci('double(double)')  # noqa: F811
    def arcsinh(x):
        return np.arcsinh(x)

    @omnisci('double(double)')  # noqa: F811
    def arccosh(x):
        return np.arccosh(x)

    @omnisci('double(double)')  # noqa: F811
    def arctanh(x):
        return np.arctanh(x)

    omnisci.register()

    for fn_name in ['arcsinh', 'arccosh', 'arctanh']:
        np_fn = getattr(np, fn_name)

        query = ''
        if fn_name == 'arccosh':
            query = 'select i, {fn_name}(i) from {omnisci.table_name}'\
                    .format(**locals())
        else:
            query = 'select x, {fn_name}(x) from {omnisci.table_name}'\
                    .format(**locals())

        descr, result = omnisci.sql_execute(query)

        for x, v in list(result):
            assert(np.isclose(np_fn(x), v))


def test_rounding_funcs(omnisci):
    omnisci.reset()

    @omnisci('double(double)')  # noqa: F811
    def around(x):
        return np.around(x)

    @omnisci('double(double)')  # noqa: F811
    def round_(x):
        return np.round_(x)

    @omnisci('double(double)')  # noqa: F811
    def floor(x):
        return np.floor(x)

    @omnisci('double(double)')  # noqa: F811
    def ceil(x):
        return np.ceil(x)

    omnisci.register()

    for fn_name in ['around', 'round_', 'floor', 'ceil']:
        np_fn = getattr(np, fn_name)

        query = ''
        if fn_name == 'arccosh':
            query = 'select i, {fn_name}(i) from {omnisci.table_name}'\
                    .format(**locals())
        else:
            query = 'select x, {fn_name}(x) from {omnisci.table_name}'\
                    .format(**locals())

        descr, result = omnisci.sql_execute(query)

        for x, v in list(result):
            assert(np.isclose(np_fn(x), v))


def test_explog_funcs(omnisci):
    omnisci.reset()

    @omnisci('double(double)')  # noqa: F811
    def exp(x):
        return np.exp(x)

    @omnisci('double(double)')  # noqa: F811
    def log(x):
        return np.log(x)

    @omnisci('double(double)')  # noqa: F811
    def log10(x):
        return np.log10(x)

    omnisci.register()

    for fn_name in ['exp', 'log', 'log10']:
        np_fn = getattr(np, fn_name)

        descr, result = omnisci.sql_execute(
            'select x, {fn_name}(x) from {omnisci.table_name}'
            .format(**locals()))

        for x, v in list(result):
            assert(np.isclose(np_fn(x), v))


def test_rational_funcs(omnisci):
    omnisci.reset()

    @omnisci('int(int, int)')  # noqa: F811
    def lcm(i, j):
        return np.lcm(i, j)

    @omnisci('int(int, int)')  # noqa: F811
    def gcd(i, j):
        return np.gcd(i, j)

    omnisci.register()

    for fn_name in ['lcm', 'gcd']:
        np_fn = getattr(np, fn_name)

        descr, result = omnisci.sql_execute(
            'select i, j, {fn_name}(i, j) from {omnisci.table_name}'
            .format(**locals()))

        for i, j, v in list(result):
            assert(np.isclose(np_fn(i, j), v))


def test_spd_funcs(omnisci):
    omnisci.reset()

    @omnisci('i32(i32[])')
    def _sum(x):
        return np.sum(x)

    @omnisci('i64(i32[])')
    def _prod(x):
        return np.prod(x)

    omnisci.register()

    for fn_name in ['_sum', '_prod']:
        np_fn = getattr(np, fn_name.lstrip('_'))

        descr, result = omnisci.sql_execute(
            'select t, {fn_name}(t) from {omnisci.table_name}'
            .format(**locals()))

        for t, v in list(result):
            assert(np.isclose(np_fn(t), v))


def test_arithmetic_funcs(omnisci):
    omnisci.reset()

    @omnisci('double(double, double)')  # noqa: F811
    def add(i, j):
        return np.add(i, j)

    @omnisci('double(double)')  # noqa: F811
    def reciprocal(x):
        return np.reciprocal(x)

    @omnisci('double(double)')  # noqa: F811
    def negative(x):
        return np.negative(x)

    @omnisci('double(double, double)')  # noqa: F811
    def multiply(x, y):
        return np.multiply(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def divide(x, y):
        return np.divide(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def powersum(x, y):
        return np.power(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def subtract(x, y):
        return np.subtract(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def true_divide(x, y):
        return np.true_divide(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def floor_divide(x, y):
        return np.floor_divide(x, y)

    @omnisci('int(int, int)')  # noqa: F811
    def mod(x, y):
        return np.mod(x, y)

    @omnisci('double(double, double)')  # noqa: F811
    def remainder(x, y):
        return np.remainder(x, y)

    omnisci.register()

    for fn_name in ['add', 'reciprocal', 'negative', 'multiply', 'divide',
                    'power', 'subtract', 'true_divide', 'floor_divide', 'mod',
                    'remainder']:
        np_fn = getattr(np, fn_name)

        if fn_name in ['reciprocal', 'negative']:
            descr, result = omnisci.sql_execute(
                'select x, {fn_name}(x) from {omnisci.table_name}'
                .format(**locals()))

            for x, v in list(result):
                assert(np.isclose(np_fn(x), v))
        elif fn_name in ['mod', 'remainder']:
            descr, result = omnisci.sql_execute(
                'select i, j, {fn_name}(i, j) from {omnisci.table_name}'
                .format(**locals()))

            for i, j, v in list(result):
                assert(np.isclose(np_fn(i, j), v))
        else:
            descr, result = omnisci.sql_execute(
                'select x, y, {fn_name}(x, y) from {omnisci.table_name}'
                .format(**locals()))

            for x, y, v in list(result):
                assert(np.isclose(np_fn(x, y), v))
