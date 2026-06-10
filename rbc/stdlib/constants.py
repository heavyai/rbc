# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Array API specification for creation functions.

https://data-apis.org/array-api/latest/API_specification/constants.html
"""
import numpy as np

__all__ = [
    'e', 'inf', 'nan', 'pi'
]


# it doesn't seem to be possible to document constants with autosummary
# https://github.com/sphinx-doc/sphinx/issues/6794
e = np.e
inf = np.inf
nan = np.nan
pi = np.pi
