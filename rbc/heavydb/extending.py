from functools import partial
from numba.core import extending
from numba.core.extending import lower_builtin

overload = partial(extending.overload, target='generic')
overload_method = partial(extending.overload_method, target='generic')
overload_attribute = partial(extending.overload_attribute, target='generic')
intrinsic = partial(extending.intrinsic, target='generic')
