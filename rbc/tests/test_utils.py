from rbc.utils import is_localhost, get_local_ip, triple_matches, check_returns_none


def test_is_localhost():
    assert is_localhost(get_local_ip())


def test_triple_matches():
    assert triple_matches('cuda', 'nvptx64-nvidia-cuda')
    assert triple_matches('nvptx64-nvidia-cuda', 'cuda')
    assert triple_matches('cuda32', 'nvptx-nvidia-cuda')
    assert triple_matches('nvptx-nvidia-cuda', 'cuda32')
    assert triple_matches('x86_64-pc-linux-gnu', 'x86_64-unknown-linux-gnu')


def test_check_returns_none():

    def foo():
        return

    assert check_returns_none(foo)

    def foo():
        return None

    assert check_returns_none(foo)

    def foo():
        pass

    assert check_returns_none(foo)

    def foo():
        if 1:
            return
        else:
            return None

    assert check_returns_none(foo)

    def foo():
        return 1

    assert not check_returns_none(foo)

    def foo():
        if 1:
            return
        else:
            return 1

    assert check_returns_none(foo)

    def foo():
        global a
        if a:
            return
        else:
            return 1

    assert not check_returns_none(foo)

    def foo(a=None):
        return a

    assert not check_returns_none(foo)

    def foo(a):
        if a:
            return a + 1
        else:
            return
    assert not check_returns_none(foo)

    def foo(a):
        pass
    assert check_returns_none(foo)

    def foo():
        a = None
        return a
    assert not check_returns_none(foo)  # false-negative
