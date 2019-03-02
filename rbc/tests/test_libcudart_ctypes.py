
import pytest

cudart = pytest.importorskip('rbc.libcudart_ctypes')

driver_version = cudart.get_cuda_versions()[0]
if driver_version == 0:
    pytest.importorskip('rbc.libcudart_ctypes_NODRIVER',
                        reason='CUDA driver not installed')


def test_get_device_count():
    assert cudart.get_device_count() > 0


def test_get_cuda_device_properties():
    props = cudart.get_cuda_device_properties(0)
    assert props['major'] >= 2
