import math
from rbc.external import external


def _register(arguments, name):
    fn = external(*arguments, name=name)
    fn.__name__ = name
    globals()[name] = fn
    return fn


def _impl(fnames, types, devices, prefixes, name, arity=1):
    arguments = []
    for fname, typ in zip(fnames, types):
        retty = typ
        argtypes = (typ,) * arity
        for device, prefix in zip(devices, prefixes):
            arguments.append(f"{retty} {prefix}{fname}({', '.join(argtypes)})|{device}")
    return _register(arguments, name)


booleans = []
booleans += [("isnand", "isnanf", math.isnan)]
booleans += [("isinfd", "isinff", math.isinf)]
booleans += [("isfinited", "finitef", math.isfinite)]

unarys = []
unarys += [("ceil", "ceilf", math.ceil)]
unarys += [("floor", "floorf", math.floor)]
unarys += [("fabs", "fabsf", math.fabs)]
unarys += [("exp", "expf", math.exp)]
unarys += [("expm1", "expm1f", math.expm1)]
unarys += [("erf", "erff", math.erf)]
unarys += [("erfc", "erfcf", math.erfc)]
unarys += [("tgamma", "tgammaf", math.gamma)]
unarys += [("lgamma", "lgammaf", math.lgamma)]
unarys += [("sqrt", "sqrtf", math.sqrt)]
unarys += [("log", "logf", math.log)]
unarys += [("log2", "log2f", math.log2)]
unarys += [("log10", "log10f", math.log10)]
unarys += [("log1p", "log1pf", math.log1p)]
unarys += [("acosh", "acoshf", math.acosh)]
unarys += [("acos", "acosf", math.acos)]
unarys += [("cos", "cosf", math.cos)]
unarys += [("cosh", "coshf", math.cosh)]
unarys += [("asinh", "asinhf", math.asinh)]
unarys += [("asin", "asinf", math.asin)]
unarys += [("sin", "sinf", math.sin)]
unarys += [("sinh", "sinhf", math.sinh)]
unarys += [("atan", "atanf", math.atan)]
unarys += [("atanh", "atanhf", math.atanh)]
unarys += [("tan", "tanf", math.tan)]
unarys += [("tanh", "tanhf", math.tanh)]
unarys += [("trunc", "truncf", math.trunc)]

binarys = []
binarys += [("copysign", "copysignf", math.copysign)]
binarys += [("atan2", "atan2f", math.atan2)]
binarys += [("pow", "powf", math.pow)]
binarys += [("fmod", "fmodf", math.fmod)]
binarys += [("hypot", "hypotf", math.hypot)]
binarys += [("remainder", "remainderf", math.remainder)]


_devices = ("CPU", "GPU")
_prefixes = ("", "__nv_")
_types = ("float32", "float64")

# for fname64, fname32, key in unarys:
#     name = getattr(key, "__name__", str(key))
#     fnames = (fname32, fname64)
#     _impl(fnames, _types, _devices, _prefixes, name, arity=1)


# for fname64, fname32, key in binarys:
#     name = getattr(key, "__name__", str(key))
#     fnames = (fname32, fname64)
#     _impl(fnames, _types, _devices, _prefixes, name, arity=2)


# # manual mapping
# ldexp = external('double ldexp(double, int32)|CPU',
#                  'float ldexpf(float, int32)|CPU',
#                  'double __nv_ldexp(double, int32)|GPU',
#                  'float __nv_ldexpf(float, int32)|GPU',
#                  name='ldexp')


# isinf = math.isinf
# isnan = math.isnan
# isfinite = math.isfinite


# pi = math.pi
# e = math.e
# tau = math.tau
# inf = math.inf
# nan = math.nan


# # CPU only
# gcd = math.gcd
# degrees = math.degrees
# radians = math.radians

# Missing:
# 'comb'
# 'dist'
# 'factorial'
# 'fsum'
# 'gcd'
# 'isclose'
# 'isqrt'
# 'frexp'
# 'modf'
# 'perm'
# 'prod'
# 'remainder'
