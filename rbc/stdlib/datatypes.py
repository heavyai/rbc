"""
Array API specification for data types.

https://data-apis.org/array-api/latest/API_specification/data_types.html
"""

__all__ = [
    'Array',
    'bool',
    'int8',
    'int16',
    'int32',
    'int64',
    'uint8',
    'uint16',
    'uint32',
    'uint64',
    'float32',
    'float64'
]

# NOTE: currently the code lives in rbc.heavydb, but eventually we
# should move it here and leave rbc.heavydb.Array only for backwards
# compatibility
from rbc.heavydb import Array

# array API data types
from numba.types import (
    boolean as bool,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
    float32,
    float64)
