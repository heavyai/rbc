from collections import namedtuple
from rbc.external import external
from rbc.typesystem import Type
from numba.core import imputils, typing

arg = namedtuple("arg", ("name", "ty"))

# Typing
typing_registry = typing.templates.Registry()
infer_global = typing_registry.register_global

# Lowering
lowering_registry = imputils.Registry()
lower = lowering_registry.lower

cmath = {
    # Trigonometric
    "cos": ("float64", [arg(name="x", ty="float64")]),
    "cosf": ("float32", [arg(name="x", ty="float32")]),
    "sin": ("float64", [arg(name="x", ty="float64")]),
    "sinf": ("float32", [arg(name="x", ty="float32")]),
    "tan": ("float64", [arg(name="x", ty="float64")]),
    "tanf": ("float32", [arg(name="x", ty="float32")]),
    "acos": ("float64", [arg(name="x", ty="float64")]),
    "acosf": ("float32", [arg(name="x", ty="float32")]),
    "asin": ("float64", [arg(name="x", ty="float64")]),
    "asinf": ("float32", [arg(name="x", ty="float32")]),
    "atan": ("float64", [arg(name="x", ty="float64")]),
    "atanf": ("float32", [arg(name="x", ty="float32")]),
    "atan2": ("float64", [arg(name="y", ty="float64"), arg(name="x", ty="float64")]),
    "atan2f": ("float32", [arg(name="y", ty="float32"), arg(name="x", ty="float32")]),
    # Hyperbolic
    "cosh": ("float64", [arg(name="x", ty="float64")]),
    "coshf": ("float32", [arg(name="x", ty="float32")]),
    "sinh": ("float64", [arg(name="x", ty="float64")]),
    "sinhf": ("float32", [arg(name="x", ty="float32")]),
    "tanh": ("float64", [arg(name="x", ty="float64")]),
    "tanhf": ("float32", [arg(name="x", ty="float32")]),
    "acosh": ("float64", [arg(name="x", ty="float64")]),
    "acoshf": ("float32", [arg(name="x", ty="float32")]),
    "asinh": ("float64", [arg(name="x", ty="float64")]),
    "asinhf": ("float32", [arg(name="x", ty="float32")]),
    "atanh": ("float64", [arg(name="x", ty="float64")]),
    "atanhf": ("float32", [arg(name="x", ty="float32")]),
    # Exponential and logarithmic functions
    "exp": ("float64", [arg(name="x", ty="float64")]),
    "expf": ("float32", [arg(name="x", ty="float32")]),
    "frexp": ("float64", [arg(name="x", ty="float64")]),
    "frexpf": ("float32", [arg(name="x", ty="float32")]),
    "ldexp": ("float64", [arg(name="x", ty="float64"), arg(name="exp", ty="int32")]),
    "ldexpf": ("float32", [arg(name="x", ty="float32"), arg(name="exp", ty="int32")]),
    "log": ("float64", [arg(name="x", ty="float64")]),
    "logf": ("float32", [arg(name="x", ty="float32")]),
    "log10": ("float64", [arg(name="x", ty="float64")]),
    "log10f": ("float32", [arg(name="x", ty="float32")]),
    "modf": ("float64", [arg(name="x", ty="float64")]),
    "modff": ("float32", [arg(name="x", ty="float32")]),
    "exp2": ("float64", [arg(name="x", ty="float64")]),
    "exp2f": ("float32", [arg(name="x", ty="float32")]),
    "expm1": ("float64", [arg(name="x", ty="float64")]),
    "expm1f": ("float32", [arg(name="x", ty="float32")]),
    "ilogb": ("float64", [arg(name="x", ty="float64")]),
    "ilogbf": ("float32", [arg(name="x", ty="float32")]),
    "log1p": ("float64", [arg(name="x", ty="float64")]),
    "log1pf": ("float32", [arg(name="x", ty="float32")]),
    "log2": ("float64", [arg(name="x", ty="float64")]),
    "log2f": ("float32", [arg(name="x", ty="float32")]),
    "logb": ("float64", [arg(name="x", ty="float64")]),
    "logbf": ("float32", [arg(name="x", ty="float32")]),
    # power functions
    "pow": (
        "float64",
        [arg(name="base", ty="float64"), arg(name="exponent", ty="float64")],
    ),
    "powf": (
        "float32",
        [arg(name="base", ty="float32"), arg(name="exponent", ty="float32")],
    ),
    "sqrt": ("float64", [arg(name="x", ty="float64")]),
    "sqrtf": ("float32", [arg(name="x", ty="float32")]),
    "cbrt": ("float64", [arg(name="x", ty="float64")]),
    "cbrtf": ("float32", [arg(name="x", ty="float32")]),
    "hypot": ("float64", [arg(name="x", ty="float64"), arg(name="y", ty="float64")]),
    "hypotf": ("float32", [arg(name="x", ty="float32"), arg(name="y", ty="float32")]),
    # error and gamma functions
    "erf": ("float64", [arg(name="x", ty="float64")]),
    "erff": ("float32", [arg(name="x", ty="float32")]),
    "erfc": ("float64", [arg(name="x", ty="float64")]),
    "erfcf": ("float32", [arg(name="x", ty="float32")]),
    "tgamma": ("float64", [arg(name="x", ty="float64")]),
    "tgammaf": ("float32", [arg(name="x", ty="float32")]),
    "lgamma": ("float64", [arg(name="x", ty="float64")]),
    "lgammaf": ("float32", [arg(name="x", ty="float32")]),
    # Rounding
    "ceil": ("float64", [arg(name="x", ty="float64")]),
    "ceilf": ("float32", [arg(name="x", ty="float32")]),
    "floor": ("float64", [arg(name="x", ty="float64")]),
    "floorf": ("float32", [arg(name="x", ty="float32")]),
    "fmod": (
        "float64",
        [arg(name="numer", ty="float64"), arg(name="denom", ty="float64")],
    ),
    "fmodf": (
        "float32",
        [arg(name="numer", ty="float32"), arg(name="denom", ty="float32")],
    ),
    "trunc": ("float64", [arg(name="x", ty="float64")]),
    "truncf": ("float32", [arg(name="x", ty="float32")]),
    "round": ("float64", [arg(name="x", ty="float64")]),
    "roundf": ("float32", [arg(name="x", ty="float32")]),
    "lround": ("int32", [arg(name="x", ty="float64")]),
    "lroundf": ("int32", [arg(name="x", ty="float32")]),
    "llround": ("int64", [arg(name="x", ty="float64")]),
    "llroundf": ("int64", [arg(name="x", ty="float32")]),
    "rint": ("float64", [arg(name="x", ty="float64")]),
    "rintf": ("float32", [arg(name="x", ty="float32")]),
    "lrint": ("int32", [arg(name="x", ty="float64")]),
    "lrintf": ("int32", [arg(name="x", ty="float32")]),
    "llrint": ("int64", [arg(name="x", ty="float64")]),
    "llrintf": ("int64", [arg(name="x", ty="float32")]),
    "nearbyint": ("float64", [arg(name="x", ty="float64")]),
    "nearbyintf": ("float32", [arg(name="x", ty="float32")]),
    "remainder": (
        "float64",
        [arg(name="numer", ty="float64"), arg(name="denom", ty="float64")],
    ),
    "remainderf": (
        "float32",
        [arg(name="numer", ty="float32"), arg(name="denom", ty="float32")],
    ),
    # Floating-point manipulation
    "copysign": (
        "float64",
        [arg(name="x", ty="float64"), arg(name="y", ty="float64")],
    ),
    "copysignf": (
        "float32",
        [arg(name="x", ty="float32"), arg(name="y", ty="float32")],
    ),
    "nan": ("float64", [arg(name="x", ty="float64")]),
    "nextafter": (
        "float64",
        [arg(name="x", ty="float64"), arg(name="y", ty="float64")],
    ),
    "nextafterf": (
        "float32",
        [arg(name="x", ty="float32"), arg(name="y", ty="float32")],
    ),
    "nexttoward": (
        "float64",
        [arg(name="x", ty="float64"), arg(name="y", ty="float64")],
    ),
    "nexttowardf": (
        "float32",
        [arg(name="x", ty="float32"), arg(name="y", ty="float32")],
    ),
    # Minimum, maximum, difference functions
    "fdim": ("float64", [arg(name="x", ty="float64"), arg(name="y", ty="float64")]),
    "fdimf": ("float32", [arg(name="x", ty="float32"), arg(name="y", ty="float32")]),
    "fmax": ("float64", [arg(name="x", ty="float64"), arg(name="y", ty="float64")]),
    "fmaxf": ("float32", [arg(name="x", ty="float32"), arg(name="y", ty="float32")]),
    "fmin": ("float64", [arg(name="x", ty="float64"), arg(name="y", ty="float64")]),
    "fminf": ("float32", [arg(name="x", ty="float32"), arg(name="y", ty="float32")]),
    # Other functions
    "fabs": ("float64", [arg(name="x", ty="float64")]),
    "fabsf": ("float32", [arg(name="x", ty="float32")]),
    "abs": ("int64", [arg(name="x", ty="float64")]),
    "fma": (
        "float64",
        [
            arg(name="x", ty="float64"),
            arg(name="y", ty="float64"),
            arg(name="z", ty="float64"),
        ],
    ),
    "fmaf": (
        "float32",
        [
            arg(name="x", ty="float32"),
            arg(name="y", ty="float32"),
            arg(name="z", ty="float32"),
        ],
    ),
}

for fname, (retty, args) in cmath.items():
    argtys = tuple(map(lambda x: x.ty, args))
    t = Type.fromstring(f"{retty} {fname}({', '.join(argtys)})")

    # expose
    s = f"def {fname}(*args): pass"
    exec(s, globals())
    key = globals()[fname]

    # typing
    class CmathTemplate(typing.templates.ConcreteTemplate):
        cases = [t.tonumba()]

    infer_global(key)(CmathTemplate)

    # lowering
    e = external(
        f"{retty} {fname}({', '.join(argtys)})|CPU", typing=False, lowering=False
    )
    lower(key, *t.tonumba().args)(e.get_codegen())
