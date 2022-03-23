# set CAPTURED_ERROS to "new_style" after issue #457 gets resolved
# https://github.com/numba/numba/pull/7397
import os
os.environ["NUMBA_CAPTURED_ERRORS"] = "old_style"

# Expose a temporary prototype. It will be replaced by proper
# implementation soon.
from .remotejit import RemoteJIT  # noqa: F401, E402
from .heavydb import RemoteHeavyDB  # noqa: F401, E402

from ._version import get_versions  # noqa: E402
__version__ = get_versions()['version']
del get_versions
