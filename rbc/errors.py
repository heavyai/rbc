"""
rbc-specific errors and warnings.
"""


from rbc.utils import get_version
from numba.core.errors import TypingError


class HeavyDBServerError(Exception):
    """
    Raised when HeavyDB server raises a runtime error that RBC knows
    how to interpret.
    """
    pass


class UnsupportedError(Exception):
    """
    Raised when an attempt to use a feature that is not supported for
    a given target. The attempt may be made both in lowering as well
    as in typing phase.
    """
    def __init__(self, *args, **kwargs):
        # numba mangles exception messages. Here we insert the exception
        # name so that irtools can demangle the messages.
        Exception.__init__(self, type(self).__name__, *args, **kwargs)


class ForbiddenNameError(Exception):
    """
    Raised when the user defines a function with name
    in a blacklist. For more info, see:
    https://github.com/xnd-project/rbc/issues/32
    """
    pass


class ForbiddenIntrinsicError(Exception):
    """
    Raised when the user uses an intrinsic with name
    in a blacklist. For more info, see:
    https://github.com/xnd-project/rbc/issues/207
    """
    pass


if get_version('numba') < (0, 55):
    class NumbaTypeError(TypingError):
        pass

    class NumbaNotImplementedError(TypingError):
        pass

    class RequireLiteralValue(TypingError):
        pass
else:
    from numba.core.errors import NumbaTypeError, NumbaNotImplementedError, \
                                  RequireLiteralValue  # noqa: F401
