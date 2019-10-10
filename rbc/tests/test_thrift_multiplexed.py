
import os
import pytest
import numpy as np
from rbc.thrift import (Server, Client, Dispatcher, Buffer, NDArray,
                        dispatchermethod, Data)
from rbc.utils import get_local_ip

socket_options = dict(host=get_local_ip(), port=6325)


class DispatcherTest(Dispatcher):

    @dispatchermethod
    def test_buffer_transport(self, buf: bytes) -> Buffer:
        assert isinstance(buf, bytes)
        return buf

    @dispatchermethod
    def test_ndarray_transport(self, arr: np.ndarray) -> NDArray:
        assert isinstance(arr, np.ndarray), repr(type(arr))
        return arr

    @dispatchermethod
    def test_ndarray_transport2(self, arr):
        assert isinstance(arr, np.ndarray), repr(type(arr))
        return arr

    @dispatchermethod
    def test_Data_transport(self, data: dict) -> Data:
        assert isinstance(data, dict), repr(type(data))
        return data

    @dispatchermethod
    def test_Data_transport2(self, data) -> Data:
        assert isinstance(data, dict), repr(type(data))
        return data

    def test_str_transport(self, s):
        assert isinstance(s, str), repr(type(s))
        return s

    def test_bool_transport(self, s):
        assert isinstance(s, bool), repr(type(s))
        return s

    def test_byte_transport(self, s):
        assert isinstance(s, int), repr(type(s))
        assert s == 8
        return s

    def test_int16_transport(self, s):
        assert isinstance(s, int), repr(type(s))
        assert s == 16
        return s

    def test_int32_transport(self, s):
        assert isinstance(s, int), repr(type(s))
        assert s == 32
        return s

    def test_int64_transport(self, s):
        assert isinstance(s, int), repr(type(s))
        assert s == 64
        return s

    @dispatchermethod
    def test_int64_transport2(self, s):
        assert isinstance(s, int), repr(type(s))
        assert s == 64
        return s

    def test_double_transport(self, s):
        assert isinstance(s, float), repr(type(s))
        assert s == 3.14
        return s

    @dispatchermethod
    def test_set_transport(self, s: set) -> set:
        assert isinstance(s, set), repr((type(s), s))
        return s

    def test_list_transport(self, s):
        assert isinstance(s, list), repr(type(s))
        return s

    def test_map_transport(self, s):
        assert isinstance(s, dict), repr(type(s))
        return s

    def test_void(self):
        return

    def test_exception(self):
        raise ValueError('my exception')

    def test_myenum_transport(self, s):
        assert isinstance(s, int), repr(type(s))
        return s


@pytest.fixture(scope="module")
def server(request):
    print('staring rpc_thrift server ...', end='')
    test_thrift_file = os.path.join(os.path.dirname(__file__),
                                    'test_multiplexed.thrift')
    ps = Server.run_bg(DispatcherTest, test_thrift_file, socket_options)

    def fin():
        if ps.is_alive():
            print('... stopping rpc_thrift server')
            ps.terminate()

    request.addfinalizer(fin)


def test_buffer_transport(server):
    conn = Client(**socket_options)
    arr = np.array([1, 2, 3, 4], dtype=np.int64)
    r = conn(test=dict(test_buffer_transport=(arr,)))
    np.testing.assert_equal(r['test']['test_buffer_transport'],
                            arr.view(np.uint8))


def test_ndarray_transport(server):
    conn = Client(**socket_options)
    arr = np.array([1, 2, 3, 4], dtype=np.int64)
    r = conn(test=dict(test_ndarray_transport=(arr,)))
    np.testing.assert_equal(r['test']['test_ndarray_transport'], arr)

    arr = np.array([[1, 2, 3, 4], [5, 6, 7, 8]], dtype=np.float64)
    r = conn(test=dict(test_ndarray_transport=(arr,)))
    np.testing.assert_equal(r['test']['test_ndarray_transport'], arr)


def test_ndarray_transport2(server):
    conn = Client(**socket_options)
    arr = np.array([1, 2, 3, 4], dtype=np.int64)
    r = conn(test=dict(test_ndarray_transport2=(arr,)))
    np.testing.assert_equal(r['test']['test_ndarray_transport2'], arr)


def test_Data_transport(server):
    conn = Client(**socket_options)
    data = dict(a=1, b=[1, 2, 3], c=dict(d=13.4))
    r = conn(test=dict(test_Data_transport=(data,)))
    assert r['test']['test_Data_transport'] == data


def test_Data_transport2(server):
    conn = Client(**socket_options)
    data = dict(a=1, b=[1, 2, 3], c=dict(d=13.4))
    r = conn(test=dict(test_Data_transport2=(data,)))
    assert r['test']['test_Data_transport2'] == data


def test_str_transport(server):
    conn = Client(**socket_options)
    s = 'hello'
    r = conn(test=dict(test_str_transport=(s,)))
    assert r['test']['test_str_transport'] == s


def test_bool_transport(server):
    conn = Client(**socket_options)
    s = True
    r = conn(test=dict(test_bool_transport=(s,)))
    assert r['test']['test_bool_transport'] == s

    s = False
    r = conn(test=dict(test_bool_transport=(s,)))
    assert r['test']['test_bool_transport'] == s


def test_byte_transport(server):
    conn = Client(**socket_options)
    s = 8
    r = conn(test=dict(test_byte_transport=(s,)))
    assert r['test']['test_byte_transport'] == s


def test_int16_transport(server):
    conn = Client(**socket_options)
    s = 16
    r = conn(test=dict(test_int16_transport=(s,)))
    assert r['test']['test_int16_transport'] == s


def test_int32_transport(server):
    conn = Client(**socket_options)
    s = 32
    r = conn(test=dict(test_int32_transport=(s,)))
    assert r['test']['test_int32_transport'] == s


def test_int64_transport(server):
    conn = Client(**socket_options)
    s = 64
    r = conn(test=dict(test_int64_transport=(s,)))
    assert r['test']['test_int64_transport'] == s


def test_int64_transport2(server):
    conn = Client(**socket_options)
    s = 64
    r = conn(test=dict(test_int64_transport2=(s,)))
    assert r['test']['test_int64_transport2'] == s


def test_double_transport(server):
    conn = Client(**socket_options)
    s = 3.14
    r = conn(test=dict(test_double_transport=(s,)))
    assert r['test']['test_double_transport'] == s


def test_set_transport(server):
    conn = Client(**socket_options)
    s = set(['abc', 'cde'])
    r = conn(test=dict(test_set_transport=(s,)))
    assert r['test']['test_set_transport'] == s


def test_list_transport(server):
    conn = Client(**socket_options)
    s = [1, 2, 3, 4]
    r = conn(test=dict(test_list_transport=(s,)))
    assert r['test']['test_list_transport'] == s


def test_map_transport(server):
    conn = Client(**socket_options)
    s = {1: 'one', 2: 'two'}
    r = conn(test=dict(test_map_transport=(s,)))
    assert r['test']['test_map_transport'] == s


def test_void(server):
    conn = Client(**socket_options)
    r = conn(test=dict(test_void=()))
    assert r['test']['test_void'] is None


def test_exception(server):
    conn = Client(**socket_options)
    with pytest.raises(
            ValueError,
            match="my exception"):
        conn(test=dict(test_exception=()))
