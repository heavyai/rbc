import math
from numba.core import types, utils
from numba.core.typing.templates import ConcreteTemplate, signature, Registry

registry = Registry()
infer_global = registry.register_global

# Adding missing cases in Numba


@infer_global(math.log2)
class Math_unary(ConcreteTemplate):
    cases = [
        signature(types.float64, types.int64),
        signature(types.float64, types.uint64),
        signature(types.float32, types.float32),
        signature(types.float64, types.float64),
    ]


if utils.PYVERSION >= (3, 7):

    @infer_global(math.remainder)
    class Math_remainder(ConcreteTemplate):
        cases = [
            signature(types.float32, types.float32, types.float32),
            signature(types.float64, types.float64, types.float64),
        ]
