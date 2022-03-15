import warnings  # noqa: F401
from .heavydb import *  # noqa: F401, F403


msg = "`RemoteOmnisci` is deprecated, use `RemoteHeavyDB` instead."
warnings.warn(msg, PendingDeprecationWarning)
