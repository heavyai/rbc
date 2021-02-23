from collections import namedtuple
from rbc.external import declare
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
    "cos": ("double", [arg(name="x", ty="double")]),
    "cosf": ("float", [arg(name="x", ty="float")]),
    "sin": ("double", [arg(name="x", ty="double")]),
    "sinf": ("float", [arg(name="x", ty="float")]),
    "tan": ("double", [arg(name="x", ty="double")]),
    "tanf": ("float", [arg(name="x", ty="float")]),
    "acos": ("double", [arg(name="x", ty="double")]),
    "acosf": ("float", [arg(name="x", ty="float")]),
    "asin": ("double", [arg(name="x", ty="double")]),
    "asinf": ("float", [arg(name="x", ty="float")]),
    "atan": ("double", [arg(name="x", ty="double")]),
    "atanf": ("float", [arg(name="x", ty="float")]),
    "atan2": ("double", [arg(name="y", ty="double"), arg(name="x", ty="double")]),
    "atan2f": ("float", [arg(name="y", ty="float"), arg(name="x", ty="float")]),
    # Hyperbolic
    "cosh": ("double", [arg(name="x", ty="double")]),
    "coshf": ("float", [arg(name="x", ty="float")]),
    "sinh": ("double", [arg(name="x", ty="double")]),
    "sinhf": ("float", [arg(name="x", ty="float")]),
    "tanh": ("double", [arg(name="x", ty="double")]),
    "tanhf": ("float", [arg(name="x", ty="float")]),
    "acosh": ("double", [arg(name="x", ty="double")]),
    "acoshf": ("float", [arg(name="x", ty="float")]),
    "asinh": ("double", [arg(name="x", ty="double")]),
    "asinhf": ("float", [arg(name="x", ty="float")]),
    "atanh": ("double", [arg(name="x", ty="double")]),
    "atanhf": ("float", [arg(name="x", ty="float")]),
    # Exponential and logarithmic functions
    "exp": ("double", [arg(name="x", ty="double")]),
    "expf": ("float", [arg(name="x", ty="float")]),
    "frexp": ("double", [arg(name="x", ty="double")]),
    "frexpf": ("float", [arg(name="x", ty="float")]),
    "ldexp": ("double", [arg(name="x", ty="double"), arg(name="exp", ty="int")]),
    "ldexpf": ("float", [arg(name="x", ty="float"), arg(name="exp", ty="int")]),
    "log": ("double", [arg(name="x", ty="double")]),
    "logf": ("float", [arg(name="x", ty="float")]),
    "log10": ("double", [arg(name="x", ty="double")]),
    "log10f": ("float", [arg(name="x", ty="float")]),
    "modf": ("double", [arg(name="x", ty="double")]),
    "modff": ("float", [arg(name="x", ty="float")]),
    "exp2": ("double", [arg(name="x", ty="double")]),
    "exp2f": ("float", [arg(name="x", ty="float")]),
    "expm1": ("double", [arg(name="x", ty="double")]),
    "expm1f": ("float", [arg(name="x", ty="float")]),
    "ilogb": ("double", [arg(name="x", ty="double")]),
    "ilogbf": ("float", [arg(name="x", ty="float")]),
    "log1p": ("double", [arg(name="x", ty="double")]),
    "log1pf": ("float", [arg(name="x", ty="float")]),
    "log2": ("double", [arg(name="x", ty="double")]),
    "log2f": ("float", [arg(name="x", ty="float")]),
    "logb": ("double", [arg(name="x", ty="double")]),
    "logbf": ("float", [arg(name="x", ty="float")]),
    # power functions
    "pow": (
        "double",
        [arg(name="base", ty="double"), arg(name="exponent", ty="double")],
    ),
    "powf": (
        "float",
        [arg(name="base", ty="float"), arg(name="exponent", ty="float")],
    ),
    "sqrt": ("double", [arg(name="x", ty="double")]),
    "sqrtf": ("float", [arg(name="x", ty="float")]),
    "cbrt": ("double", [arg(name="x", ty="double")]),
    "cbrtf": ("float", [arg(name="x", ty="float")]),
    "hypot": ("double", [arg(name="x", ty="double"), arg(name="y", ty="double")]),
    "hypotf": ("float", [arg(name="x", ty="float"), arg(name="y", ty="float")]),
    # error and gamma functions
    "erf": ("double", [arg(name="x", ty="double")]),
    "erff": ("float", [arg(name="x", ty="float")]),
    "erfc": ("double", [arg(name="x", ty="double")]),
    "erfcf": ("float", [arg(name="x", ty="float")]),
    "tgamma": ("double", [arg(name="x", ty="double")]),
    "tgammaf": ("float", [arg(name="x", ty="float")]),
    "lgamma": ("double", [arg(name="x", ty="double")]),
    "lgammaf": ("float", [arg(name="x", ty="float")]),
    # Rounding
    "ceil": ("double", [arg(name="x", ty="double")]),
    "ceilf": ("float", [arg(name="x", ty="float")]),
    "floor": ("double", [arg(name="x", ty="double")]),
    "floorf": ("float", [arg(name="x", ty="float")]),
    "fmod": (
        "double",
        [arg(name="numer", ty="double"), arg(name="denom", ty="double")],
    ),
    "fmodf": (
        "float",
        [arg(name="numer", ty="float"), arg(name="denom", ty="float")],
    ),
    "trunc": ("double", [arg(name="x", ty="double")]),
    "truncf": ("float", [arg(name="x", ty="float")]),
    "round": ("double", [arg(name="x", ty="double")]),
    "roundf": ("float", [arg(name="x", ty="float")]),
    "lround": ("long int", [arg(name="x", ty="double")]),
    "lroundf": ("long int", [arg(name="x", ty="float")]),
    "llround": ("long long int", [arg(name="x", ty="double")]),
    "llroundf": ("long long int", [arg(name="x", ty="float")]),
    "rint": ("double", [arg(name="x", ty="double")]),
    "rintf": ("float", [arg(name="x", ty="float")]),
    "lrint": ("long int", [arg(name="x", ty="double")]),
    "lrintf": ("long int", [arg(name="x", ty="float")]),
    "llrint": ("long long int", [arg(name="x", ty="double")]),
    "llrintf": ("long long int", [arg(name="x", ty="float")]),
    "nearbyint": ("double", [arg(name="x", ty="double")]),
    "nearbyintf": ("float", [arg(name="x", ty="float")]),
    "remainder": (
        "double",
        [arg(name="numer", ty="double"), arg(name="denom", ty="double")],
    ),
    "remainderf": (
        "float",
        [arg(name="numer", ty="float"), arg(name="denom", ty="float")],
    ),
    # Floating-point manipulation
    "copysign": (
        "double",
        [arg(name="x", ty="double"), arg(name="y", ty="double")],
    ),
    "copysignf": (
        "float",
        [arg(name="x", ty="float"), arg(name="y", ty="float")],
    ),
    "nan": ("double", [arg(name="x", ty="double")]),
    "nextafter": (
        "double",
        [arg(name="x", ty="double"), arg(name="y", ty="double")],
    ),
    "nextafterf": (
        "float",
        [arg(name="x", ty="float"), arg(name="y", ty="float")],
    ),
    "nexttoward": (
        "double",
        [arg(name="x", ty="double"), arg(name="y", ty="double")],
    ),
    "nexttowardf": (
        "float",
        [arg(name="x", ty="float"), arg(name="y", ty="float")],
    ),
    # Minimum, maximum, difference functions
    "fdim": ("double", [arg(name="x", ty="double"), arg(name="y", ty="double")]),
    "fdimf": ("float", [arg(name="x", ty="float"), arg(name="y", ty="float")]),
    "fmax": ("double", [arg(name="x", ty="double"), arg(name="y", ty="double")]),
    "fmaxf": ("float", [arg(name="x", ty="float"), arg(name="y", ty="float")]),
    "fmin": ("double", [arg(name="x", ty="double"), arg(name="y", ty="double")]),
    "fminf": ("float", [arg(name="x", ty="float"), arg(name="y", ty="float")]),
    # Other functions
    "fabs": ("double", [arg(name="x", ty="double")]),
    "fabsf": ("float", [arg(name="x", ty="float")]),
    "abs": ("long long", [arg(name="x", ty="double")]),
    "absf": ("long", [arg(name="x", ty="float")]),
    "fma": (
        "double",
        [
            arg(name="x", ty="double"),
            arg(name="y", ty="double"),
            arg(name="z", ty="double"),
        ],
    ),
    "fmaf": (
        "float",
        [
            arg(name="x", ty="float"),
            arg(name="y", ty="float"),
            arg(name="z", ty="float"),
        ],
    ),
}


def register(fname, retty, argtys):

    # expose
    s = f"def {fname}(*args): pass"
    exec(s, globals())
    _key = globals()[fname]

    # typing
    @infer_global(_key)
    class CMathTemplate(typing.templates.AbstractTemplate):
        key = _key

        def generic(self, args, kws):
            # get the correct signature and function name for the current device
            atypes = tuple(map(Type.fromobject, args))
            e = declare(f"{retty} {fname}({', '.join(argtys)})|CPU")

            t = e.match_signature(atypes)

            codegen = e.get_codegen()
            lower(_key, *t.tonumba().args)(codegen)

            return t.tonumba()


for fname, (retty, args) in cmath.items():
    argtys = tuple(map(lambda x: x.ty, args))
    register(fname, retty, argtys)
    exec(f"{fname} = declare('{retty} {fname}({', '.join(argtys)})|CPU')", globals())
