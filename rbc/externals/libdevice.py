"""https://docs.nvidia.com/cuda/libdevice-users-guide/index.html
"""

from . import register_external
from numba.core import imputils, typing  # noqa: E402
from numba.cuda import libdevicefuncs  # noqa: E402

# Typing
typing_registry = typing.templates.Registry()

# Lowering
lowering_registry = imputils.Registry()

for fname, (retty, args) in libdevicefuncs.functions.items():
    doc = f"libdevice function {fname}"
    fn = register_external(
        fname, retty, args, __name__, globals(), typing_registry, lowering_registry, doc
    )

    fn.__name__ = fname  # for sphinx
