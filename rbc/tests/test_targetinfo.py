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
