import math
from numba.core import types
from numba.core.typing.templates import ConcreteTemplate, signature, Registry

registry = Registry()
infer_global = registry.register_global

# Adding missing cases in Numba
@infer_global(math.log2)  # noqa: E302
class Math_unary(ConcreteTemplate):
    cases = [
        signature(types.float64, types.int64),
        signature(types.float64, types.uint64),
        signature(types.float32, types.float32),
        signature(types.float64, types.float64),
    ]


@infer_global(math.remainder)
class Math_remainder(ConcreteTemplate):
    cases = [
        signature(types.float32, types.float32, types.float32),
        signature(types.float64, types.float64, types.float64),
    ]


@infer_global(math.floor)
@infer_global(math.trunc)
@infer_global(math.ceil)
class Math_converter(ConcreteTemplate):
    cases = [
        signature(types.intp, types.intp),
        signature(types.int64, types.int64),
        signature(types.uint64, types.uint64),
        signature(types.float32, types.float32),
        signature(types.float64, types.float64),
    ]
