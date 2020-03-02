import atexit
import pytest
from rbc.remotejit import RemoteJIT
import numpy as np


@pytest.fixture(scope="module")
def rjit(request):
    rjit = RemoteJIT()
    rjit.start_server(background=True)
    request.addfinalizer(rjit.stop_server)
    atexit.register(rjit.stop_server)
    return rjit


def test_trunc(rjit):
    @rjit('double(double)')
    def trunc(x):
        return np.trunc(x)

    assert (np.allclose(trunc(0.3), np.trunc(0.3)))
    assert (np.allclose(trunc(2.9), np.trunc(2.0)))


def test_exp2(rjit):
    @rjit('double(double)')
    def exp2(x):
        return np.exp2(x)

    assert (np.allclose(exp2(2), np.exp2(2)))
    assert (np.allclose(exp2(3), np.exp2(3)))


def test_logaddexp(rjit):
    # ref:
    # https://github.com/numpy/numpy/blob/2fd9ff8277ad25aa386c3432b6ebc35322879d91/numpy/core/tests/test_umath.py#L818-L860

    @rjit('double(double, double)')
    def logaddexp(x, y):
        return np.logaddexp(x, y)

    def test_values():
        x = [1, 2, 3, 4, 5]
        y = [5, 4, 3, 2, 1]
        z = [6, 6, 6, 6, 6]
        for _x, _y, _z in zip(x, y, z):
            for dt, dec_ in zip(['f', 'd', 'g'], [6, 15, 15]):
                logxf = np.log(np.array(_x, dtype=dt))
                logyf = np.log(np.array(_y, dtype=dt))
                logzf = np.log(np.array(_z, dtype=dt))
                np.allclose(logaddexp(logxf, logyf), logzf)

    def test_range():
        x = [1000000, -1000000, 1000200, -1000200]
        y = [1000200, -1000200, 1000000, -1000000]
        z = [1000200, -1000000, 1000200, -1000000]
        for _x, _y, _z in zip(x, y, z):
            for dt in ['f', 'd', 'g']:
                # [()] ~> quick hack! rbc doesn't support passing arrays
                logxf = np.array(_x, dtype=dt)[()]
                logyf = np.array(_y, dtype=dt)[()]
                logzf = np.array(_z, dtype=dt)[()]
                np.allclose(logaddexp(logxf, logyf), logzf)

    def test_inf():
        # logaddexp inf
        inf = np.inf
        x = [inf, -inf,  inf, -inf, inf, 1,  -inf,  1]
        y = [inf,  inf, -inf, -inf, 1,   inf, 1,   -inf]
        z = [inf,  inf,  inf, -inf, inf, inf, 1,    1]
        with np.errstate(invalid='raise'):
            for _x, _y, _z in zip(x, y, z):
                for dt in ['f', 'd', 'g']:
                    # [()] ~> quick hack! rbc doesn't support passing arrays
                    logxf = np.array(_x, dtype=dt)[()]
                    logyf = np.array(_y, dtype=dt)[()]
                    logzf = np.array(_z, dtype=dt)[()]
                    np.allclose(np.logaddexp(logxf, logyf), logzf)

    test_values()
    test_range()
    test_inf()
