"""
Array API specification for data types.

https://data-apis.org/array-api/latest/API_specification/data_types.html

Datatypes exported
------------------

+------------+
|   Types    |
+============+
|    bool    |
+------------+
|    int8    |
+------------+
|    int16   |
+------------+
|    int32   |
+------------+
|    int64   |
+------------+
|    uint8   |
+------------+
|    uint16  |
+------------+
|    uint32  |
+------------+
|    uint64  |
+------------+
|  float32   |
+------------+
|  float64   |
+------------+
| complex64  |
+------------+
| complex128 |
+------------+

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
    'float64',
    'complex64',
    'complex128',
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
    float64,
    complex64,
    complex128)
