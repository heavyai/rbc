"""
rbc-specific errors and warnings.
"""

import numba
from .utils import get_version

if get_version('numba') >= (0, 49):
    numba_errors = numba.core.errors
else:
    numba_errors = numba.errors


class OmnisciServerError(Exception):
    """
    Launch when OmnisciDB server raises a runtime error that RBC knows
    how to interpret.
    """


class UnsupportedError(numba_errors.UnsupportedError):
    """
    Launch when an attempt is to use a feature that is not supported
    for a given target.
    """


class ForbiddenNameError(Exception):
    """
    Launch when the user defines a function with name
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
