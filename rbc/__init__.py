import os
os.environ["NUMBA_CAPTURED_ERRORS"] = "new_style"

# Expose a temporary prototype. It will be replaced by proper
# implementation soon.
from .remotejit import RemoteJIT  # noqa: F401, E402
from .heavydb import RemoteHeavyDB  # noqa: F401, E402

from ._version import get_versions  # noqa: E402
__version__ = get_versions()['version']
del get_versions
