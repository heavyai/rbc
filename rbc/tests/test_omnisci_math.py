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
        'CREATE TABLE IF NOT EXISTS {table_name} (x DOUBLE, y DOUBLE, i INT);'
        .format(**locals()))

    for _i in range(1, 6):
        x = _i/10.0
        y = _i/6.0
        i = _i
        m.sql_execute('insert into {table_name} values ({x}, {y}, {i})'
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

    # fails
    @omnisci('double(double, double)')  # noqa: F811
    def hypot(x, y):
        # z = np.hypot(x, y)
        return 0.0

    @omnisci('double(double, double)')  # noqa: F811
    def arctan2(x, y):
        return np.arctan2(x, y)

    omnisci.register()

    for fn_name in ['sin', 'cos', 'tan', 'arcsin',
                    'arccos', 'arctan', 'arctan2']:
        np_fn = getattr(np, fn_name)

        if fn_name in ['arctan2']:
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

    # @omnisci('double(double)')  # noqa: F811
    # def sinh(x):
    #     return np.sinh(x)

    # @omnisci('double(double)')  # noqa: F811
    # def cosh(x):
    #     return np.cosh(x)

    # @omnisci('double(double)')  # noqa: F811
    # def tanh(x):
    #     return np.tanh(x)

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
