import math
from rbc.external import declare
from numba.core import imputils
from numba.core.typing.templates import ConcreteTemplate, signature, Registry
from numba.types import float32, float64, int32, int64, uint64, intp


# Typing
typing_registry = Registry()
infer_global = typing_registry.register_global

# Adding missing cases in Numba
@infer_global(math.log2)  # noqa: E302
class Math_unary(ConcreteTemplate):
    cases = [
        signature(float64, int64),
        signature(float64, uint64),
        signature(float32, float32),
        signature(float64, float64),
    ]


@infer_global(math.remainder)
class Math_remainder(ConcreteTemplate):
    cases = [
        signature(float32, float32, float32),
        signature(float64, float64, float64),
    ]


@infer_global(math.floor)
@infer_global(math.trunc)
@infer_global(math.ceil)
class Math_converter(ConcreteTemplate):
    cases = [
        signature(intp, intp),
        signature(int64, int64),
        signature(uint64, uint64),
        signature(float32, float32),
        signature(float64, float64),
    ]


# Lowering
lowering_registry = imputils.Registry()
lower = lowering_registry.lower


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


def gen_external(
    fname, retty, argtypes, devices=("CPU", "GPU"), prefixes=("", "__nv_")
):
    arguments = []
    for device, prefix in zip(devices, prefixes):
        arguments.append(
            f"{retty} {prefix}{fname}({', '.join(map(str, argtypes))})|{device}"
        )
    return declare(*arguments)


def impl_unary(fname, key, typ):
    e = gen_external(fname, typ, (typ,))
    lower(key, typ)(e.get_codegen())


def impl_binary(fname, key, typ):
    e = gen_external(fname, typ, (typ, typ))
    lower(key, typ, typ)(e.get_codegen())


for fname64, fname32, key in unarys:
    impl_unary(fname64, key, float64)
    impl_unary(fname32, key, float32)


for fname64, fname32, key in binarys:
    impl_binary(fname64, key, float64)
    impl_binary(fname32, key, float32)


# manual mapping
def impl_ldexp():
    ldexp = declare(
        "double ldexp(double, int32)|CPU",
        "double __nv_ldexp(double, int32)|GPU",
    )

    ldexpf = declare(
        "float ldexpf(float, int32)|CPU",
        "float __nv_ldexpf(float, int32)|GPU",
    )

    lower(math.ldexp, float64, int32)(ldexp.get_codegen())
    lower(math.ldexp, float32, int32)(ldexpf.get_codegen())


impl_ldexp()

# CPU only:
# math.gcd
# math.degrees
# math.radians

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
