import warnings

from rbc.heavydb import RemoteHeavyDB


class RemoteOmnisci(RemoteHeavyDB):
    """Omnisci - the previous brand of HeavyAI
    """
    msg = "`RemoteOmnisci` is deprecated, use `RemoteHeavyDB` instead."
    warnings.warn(msg, PendingDeprecationWarning)
