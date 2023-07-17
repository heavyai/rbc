"""
Array API specification for data types.

https://data-apis.org/array-api/latest/API_specification/data_types.html
"""

__all__ = [
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
