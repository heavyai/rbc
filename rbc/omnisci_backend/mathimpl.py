import math
from rbc.externals import gen_codegen, dispatch_codegen
from numba.core.typing.templates import infer_global
from numba.core.imputils import lower_builtin
from numba.core.typing.templates import ConcreteTemplate, signature
from numba.types import float32, float64, int32, int64, uint64, intp


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


def impl_unary(fname, key, typ):
    cpu = gen_codegen(fname)
    gpu = gen_codegen(f"__nv_{fname}")
    lower_builtin(key, typ)(dispatch_codegen(cpu, gpu))


def impl_binary(fname, key, typ):
    cpu = gen_codegen(fname)
    gpu = gen_codegen(f"__nv_{fname}")
    lower_builtin(key, typ, typ)(dispatch_codegen(cpu, gpu))


for fname64, fname32, key in unarys:
    impl_unary(fname64, key, float64)
    impl_unary(fname32, key, float32)


for fname64, fname32, key in binarys:
    impl_binary(fname64, key, float64)
    impl_binary(fname32, key, float32)


# manual mapping
def impl_ldexp():
    ldexp_cpu = gen_codegen('ldexp')
    ldexp_gpu = gen_codegen('__nv_ldexp')

    ldexpf_cpu = gen_codegen('ldexpf')
    ldexpf_gpu = gen_codegen('__nv_ldexpf')

    lower_builtin(math.ldexp, float64, int32)(dispatch_codegen(ldexp_cpu, ldexp_gpu))
    lower_builtin(math.ldexp, float32, int32)(dispatch_codegen(ldexpf_cpu, ldexpf_gpu))


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
