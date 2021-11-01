"""
rbc-specific errors and warnings.
"""


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
