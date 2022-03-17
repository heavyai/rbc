import warnings  # noqa: F401
from .heavydb import *  # noqa: F401, F403


msg = "`import rbc.omniscidb` is deprecated, use `import rbc.heavyai` instead."
warnings.warn(msg, PendingDeprecationWarning)
