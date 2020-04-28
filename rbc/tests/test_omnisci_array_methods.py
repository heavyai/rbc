import pytest


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    yield m


def test_array_fill(omnisci):
    omnisci.reset()

    from rbc.omnisci_array import Array

    @omnisci('double(int64, double)')
    def array_fill(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return sum(a)

    query = (
        'select array_fill(3, 5);'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    assert list(result)[0] == (15.0,)


def test_array_sum(omnisci):
    omnisci.reset()

    from rbc.omnisci_array import Array

    @omnisci('double(int64)')
    def array_sum(size):
        a = Array(size, 'double')
        a.fill(1.0)
        return a.sum()

    query = (
        'select array_sum(3);'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    assert list(result)[0] == (3.0,)


def test_array_prod(omnisci):
    omnisci.reset()

    from rbc.omnisci_array import Array

    @omnisci('double(int64, double)')
    def array_prod(size, v):
        a = Array(size, 'double')
        a.fill(v)
        return a.prod()

    query = (
        'select array_prod(3, 5);'
        .format(**locals()))
    _, result = omnisci.sql_execute(query)

    assert list(result)[0] == (125.0,)
