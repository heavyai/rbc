from rbc.utils import is_localhost, get_local_ip, triple_matches


def test_is_localhost():
    assert is_localhost(get_local_ip())


def test_triple_matches():
    assert triple_matches('cuda', 'nvptx64-nvidia-cuda')
    assert triple_matches('nvptx64-nvidia-cuda', 'cuda')
    assert triple_matches('cuda32', 'nvptx-nvidia-cuda')
    assert triple_matches('nvptx-nvidia-cuda', 'cuda32')
    assert triple_matches('x86_64-pc-linux-gnu', 'x86_64-unknown-linux-gnu')
