"""https://en.cppreference.com/w/c/numeric/math
"""


from collections import namedtuple
from . import make_intrinsic

_arg = namedtuple("arg", ("name", "ty"))

cmath = {
    # Trigonometric
    "cos": ("double", [_arg(name="x", ty="double")]),
    "cosf": ("float", [_arg(name="x", ty="float")]),
    "sin": ("double", [_arg(name="x", ty="double")]),
    "sinf": ("float", [_arg(name="x", ty="float")]),
    "tan": ("double", [_arg(name="x", ty="double")]),
    "tanf": ("float", [_arg(name="x", ty="float")]),
    "acos": ("double", [_arg(name="x", ty="double")]),
    "acosf": ("float", [_arg(name="x", ty="float")]),
    "asin": ("double", [_arg(name="x", ty="double")]),
    "asinf": ("float", [_arg(name="x", ty="float")]),
    "atan": ("double", [_arg(name="x", ty="double")]),
    "atanf": ("float", [_arg(name="x", ty="float")]),
    "atan2": ("double", [_arg(name="y", ty="double"), _arg(name="x", ty="double")]),
    "atan2f": ("float", [_arg(name="y", ty="float"), _arg(name="x", ty="float")]),
    # Hyperbolic
    "cosh": ("double", [_arg(name="x", ty="double")]),
    "coshf": ("float", [_arg(name="x", ty="float")]),
    "sinh": ("double", [_arg(name="x", ty="double")]),
    "sinhf": ("float", [_arg(name="x", ty="float")]),
    "tanh": ("double", [_arg(name="x", ty="double")]),
    "tanhf": ("float", [_arg(name="x", ty="float")]),
    "acosh": ("double", [_arg(name="x", ty="double")]),
    "acoshf": ("float", [_arg(name="x", ty="float")]),
    "asinh": ("double", [_arg(name="x", ty="double")]),
    "asinhf": ("float", [_arg(name="x", ty="float")]),
    "atanh": ("double", [_arg(name="x", ty="double")]),
    "atanhf": ("float", [_arg(name="x", ty="float")]),
    # Exponential and logarithmic functions
    "exp": ("double", [_arg(name="x", ty="double")]),
    "expf": ("float", [_arg(name="x", ty="float")]),
    "frexp": ("double", [_arg(name="x", ty="double"), _arg(name="exp", ty="int*")]),
    "frexpf": ("float", [_arg(name="x", ty="float"), _arg(name="exp", ty="int*")]),
    "ldexp": ("double", [_arg(name="x", ty="double"), _arg(name="exp", ty="int")]),
    "ldexpf": ("float", [_arg(name="x", ty="float"), _arg(name="exp", ty="int")]),
    "log": ("double", [_arg(name="x", ty="double")]),
    "logf": ("float", [_arg(name="x", ty="float")]),
    "log10": ("double", [_arg(name="x", ty="double")]),
    "log10f": ("float", [_arg(name="x", ty="float")]),
    "modf": ("double", [_arg(name="x", ty="double"), _arg(name="intpart", ty="double*")]),
    "modff": ("float", [_arg(name="x", ty="float"), _arg(name="intpart", ty="float*")]),
    "exp2": ("double", [_arg(name="x", ty="double")]),
    "exp2f": ("float", [_arg(name="x", ty="float")]),
    "expm1": ("double", [_arg(name="x", ty="double")]),
    "expm1f": ("float", [_arg(name="x", ty="float")]),
    "ilogb": ("double", [_arg(name="x", ty="double")]),
    "ilogbf": ("float", [_arg(name="x", ty="float")]),
    "log1p": ("double", [_arg(name="x", ty="double")]),
    "log1pf": ("float", [_arg(name="x", ty="float")]),
    "log2": ("double", [_arg(name="x", ty="double")]),
    "log2f": ("float", [_arg(name="x", ty="float")]),
    "logb": ("double", [_arg(name="x", ty="double")]),
    "logbf": ("float", [_arg(name="x", ty="float")]),
    # power functions
    "pow": (
        "double",
        [_arg(name="base", ty="double"), _arg(name="exponent", ty="double")],
    ),
    "powf": (
        "float",
        [_arg(name="base", ty="float"), _arg(name="exponent", ty="float")],
    ),
    "sqrt": ("double", [_arg(name="x", ty="double")]),
    "sqrtf": ("float", [_arg(name="x", ty="float")]),
    "cbrt": ("double", [_arg(name="x", ty="double")]),
    "cbrtf": ("float", [_arg(name="x", ty="float")]),
    "hypot": ("double", [_arg(name="x", ty="double"), _arg(name="y", ty="double")]),
    "hypotf": ("float", [_arg(name="x", ty="float"), _arg(name="y", ty="float")]),
    # error and gamma functions
    "erf": ("double", [_arg(name="x", ty="double")]),
    "erff": ("float", [_arg(name="x", ty="float")]),
    "erfc": ("double", [_arg(name="x", ty="double")]),
    "erfcf": ("float", [_arg(name="x", ty="float")]),
    "tgamma": ("double", [_arg(name="x", ty="double")]),
    "tgammaf": ("float", [_arg(name="x", ty="float")]),
    "lgamma": ("double", [_arg(name="x", ty="double")]),
    "lgammaf": ("float", [_arg(name="x", ty="float")]),
    # Rounding
    "ceil": ("double", [_arg(name="x", ty="double")]),
    "ceilf": ("float", [_arg(name="x", ty="float")]),
    "floor": ("double", [_arg(name="x", ty="double")]),
    "floorf": ("float", [_arg(name="x", ty="float")]),
    "fmod": (
        "double",
        [_arg(name="numer", ty="double"), _arg(name="denom", ty="double")],
    ),
    "fmodf": (
        "float",
        [_arg(name="numer", ty="float"), _arg(name="denom", ty="float")],
    ),
    "trunc": ("double", [_arg(name="x", ty="double")]),
    "truncf": ("float", [_arg(name="x", ty="float")]),
    "round": ("double", [_arg(name="x", ty="double")]),
    "roundf": ("float", [_arg(name="x", ty="float")]),
    "lround": ("long int", [_arg(name="x", ty="double")]),
    "lroundf": ("long int", [_arg(name="x", ty="float")]),
    "llround": ("long long int", [_arg(name="x", ty="double")]),
    "llroundf": ("long long int", [_arg(name="x", ty="float")]),
    "rint": ("double", [_arg(name="x", ty="double")]),
    "rintf": ("float", [_arg(name="x", ty="float")]),
    "lrint": ("long int", [_arg(name="x", ty="double")]),
    "lrintf": ("long int", [_arg(name="x", ty="float")]),
    "llrint": ("long long int", [_arg(name="x", ty="double")]),
    "llrintf": ("long long int", [_arg(name="x", ty="float")]),
    "nearbyint": ("double", [_arg(name="x", ty="double")]),
    "nearbyintf": ("float", [_arg(name="x", ty="float")]),
    "remainder": (
        "double",
        [_arg(name="numer", ty="double"), _arg(name="denom", ty="double")],
    ),
    "remainderf": (
        "float",
        [_arg(name="numer", ty="float"), _arg(name="denom", ty="float")],
    ),
    # Floating-point manipulation
    "copysign": (
        "double",
        [_arg(name="x", ty="double"), _arg(name="y", ty="double")],
    ),
    "copysignf": (
        "float",
        [_arg(name="x", ty="float"), _arg(name="y", ty="float")],
    ),
    "nan": ("double", [_arg(name="tagp", ty="const char*")]),
    "nanf": ("float", [_arg(name="tagp", ty="const char*")]),
    "nextafter": (
        "double",
        [_arg(name="x", ty="double"), _arg(name="y", ty="double")],
    ),
    "nextafterf": (
        "float",
        [_arg(name="x", ty="float"), _arg(name="y", ty="float")],
    ),
    "nexttoward": (
        "double",
        [_arg(name="x", ty="double"), _arg(name="y", ty="double")],
    ),
    "nexttowardf": (
        "float",
        [_arg(name="x", ty="float"), _arg(name="y", ty="float")],
    ),
    # Minimum, maximum, difference functions
    "fdim": ("double", [_arg(name="x", ty="double"), _arg(name="y", ty="double")]),
    "fdimf": ("float", [_arg(name="x", ty="float"), _arg(name="y", ty="float")]),
    "fmax": ("double", [_arg(name="x", ty="double"), _arg(name="y", ty="double")]),
    "fmaxf": ("float", [_arg(name="x", ty="float"), _arg(name="y", ty="float")]),
    "fmin": ("double", [_arg(name="x", ty="double"), _arg(name="y", ty="double")]),
    "fminf": ("float", [_arg(name="x", ty="float"), _arg(name="y", ty="float")]),
    # Other functions
    "fabs": ("double", [_arg(name="x", ty="double")]),
    "fabsf": ("float", [_arg(name="x", ty="float")]),
    "abs": ("long long int", [_arg(name="x", ty="double")]),
    "absf": ("long", [_arg(name="x", ty="float")]),
    "fma": (
        "double",
        [
            _arg(name="x", ty="double"),
            _arg(name="y", ty="double"),
            _arg(name="z", ty="double"),
        ],
    ),
    "fmaf": (
        "float",
        [
            _arg(name="x", ty="float"),
            _arg(name="y", ty="float"),
            _arg(name="z", ty="float"),
        ],
    ),
}


for fname, (retty, args) in cmath.items():
    argnames = [arg.name for arg in args]
    doc = f"C math function {fname}"
    fn = make_intrinsic(fname, retty, argnames, __name__, globals(), doc)
