from rbc.targetinfo import TargetInfo


def test_basic():
    host = TargetInfo.host()
    assert host.name == 'host_cpu'
    with host:
        ti = TargetInfo()
        assert ti is host


def test_to_from_dict():
    host = TargetInfo.host()
    d = host.todict()
    host2 = TargetInfo.fromdict(d)
    assert host.__dict__ == host2.__dict__


def test_host_cache():
    host = TargetInfo.host()
    host2 = TargetInfo.host()
    assert host is host2


def test_allocation_functions():
    host = TargetInfo.host()
    dhost = TargetInfo.host(use_tracing_allocator=True)
    assert host is not dhost  # check that use_tracing_allocator is part of the cache key
    assert host.info['fn_allocate_varlen_buffer'] == 'rbclib_allocate_varlen_buffer'
    assert host.info['fn_free_buffer'] == 'rbclib_free_buffer'
    #
    assert dhost.info['fn_allocate_varlen_buffer'] == 'rbclib_tracing_allocate_varlen_buffer'
    assert dhost.info['fn_free_buffer'] == 'rbclib_tracing_free_buffer'
