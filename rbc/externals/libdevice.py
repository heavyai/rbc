# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""https://docs.nvidia.com/cuda/libdevice-users-guide/index.html
"""

from . import make_intrinsic
from numba.cuda import libdevicefuncs  # noqa: E402

for fname, (retty, args) in libdevicefuncs.functions.items():
    argnames = [arg.name for arg in args]
    doc = f"libdevice function {fname}"
    fn = make_intrinsic(fname, retty, argnames, __name__, globals(), doc)
