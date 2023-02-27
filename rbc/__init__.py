import os
os.environ["NUMBA_CAPTURED_ERRORS"] = "new_style"
os.environ["NUMBA_LOOP_VECTORIZE"] = "0"
os.environ["NUMBA_SLP_VECTORIZE"] = "0"
# TODO: line below is just for debugging purposes
os.environ["NUMBA_OPT"] = "1"
# os.environ["RBC_DEBUG"] = "0"
# os.environ["RBC_DEBUG_NRT"] = "0"

# Expose a temporary prototype. It will be replaced by proper
# implementation soon.
from .remotejit import RemoteJIT  # noqa: F401, E402
from .heavydb import RemoteHeavyDB  # noqa: F401, E402

from ._version import get_versions  # noqa: E402
__version__ = get_versions()['version']
del get_versions
