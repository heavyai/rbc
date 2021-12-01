"""
rbc-specific errors and warnings.
"""


from rbc.utils import get_version
from numba.core.errors import TypingError


class OmnisciServerError(Exception):
    """
    Raised when OmnisciDB server raises a runtime error that RBC knows
    how to interpret.
    """
    pass


class UnsupportedError(Exception):
    """
    Raised when an attempt to use a feature that is not supported
    for a given target.
    """
    pass


if get_version('numba') < (0, 55):
    class NumbaTypeError(TypingError):
        pass

    class NumbaNotImplementedError(TypingError):
        pass
else:
    from numba.core.errors import NumbaTypeError, NumbaNotImplementedError  # noqa: F401
