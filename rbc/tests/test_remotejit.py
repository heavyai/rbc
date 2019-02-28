
import pytest
from rbc.remotejit import RemoteJIT, Signature
from rbc.caller import Caller
from rbc.typesystem import Type


def test_construction():

    rjit = RemoteJIT()
    assert isinstance(rjit, RemoteJIT)

    # Case 1

    @rjit
    def add(a: int, b: int) -> int:
        return a + b

    assert isinstance(add, Caller)
    assert len(add._signatures) == 1
    assert add._signatures[0] == Type.fromstring('i64(i64,i64)')

    # Case 2

    @rjit('double(double, double)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    assert len(add._signatures) == 1
    assert add._signatures[0] == Type.fromstring('f64(f64,f64)')

    # Case 3

    @rjit('double(double, double)')
    @rjit('int(int, int)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    assert len(add._signatures) == 2
    assert add._signatures[0] == Type.fromstring('i32(i32,i32)')
    assert add._signatures[1] == Type.fromstring('f64(f64,f64)')

    # Case 4

    @rjit('double(double, double)',
          'int(int, int)')
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    assert len(add._signatures) == 2
    assert add._signatures[0] == Type.fromstring('f64(f64,f64)')
    assert add._signatures[1] == Type.fromstring('i32(i32,i32)')

    # Case 5

    rjit_int = rjit('int(int, int)')
    rjit_double = rjit('double(double, double)')
    assert isinstance(rjit_int, Signature)

    rjit_types = rjit_int(rjit_double)
    assert isinstance(rjit_types, Signature)

    @rjit_types
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    assert len(add._signatures) == 2
    assert add._signatures[0] == Type.fromstring('i32(i32,i32)')
    assert add._signatures[1] == Type.fromstring('f64(f64,f64)')

    # Case 6

    @rjit
    def add(a, b):
        return a + b

    assert isinstance(add, Caller)
    assert len(add._signatures) == 0

    add.add_signature('i32(i32,i32)')
    assert len(add._signatures) == 1
    assert add._signatures[0] == Type.fromstring('i32(i32,i32)')

    # Invalid cases

    with pytest.raises(ValueError,
                       match=r'mismatch of the number of arguments'):
        @rjit('int32(int32)')
        def add(a, b):
            return a + b

    with pytest.raises(ValueError,
                       match=r'expected signature with function kind'):
        @rjit(int)
        def add(a, b):
            return a + b
