"""
rbc-specific errors and warnings.
"""


class ForbiddenNameError(Exception):
    """
    Launch when the user defines a function with name
    in a blacklist. For more info, see:
    https://github.com/xnd-project/rbc/issues/32
    """
    pass
