# Expose a temporary prototype. It will be replaced by proper
# implementation soon.
from .remotejit import RemoteJIT  # noqa: F401
from .heavydb import RemoteHeavyDB  # noqa: F401

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
