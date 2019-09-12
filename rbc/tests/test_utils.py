from rbc.utils import is_localhost, get_local_ip


def test_is_localhost():
    assert is_localhost(get_local_ip())
