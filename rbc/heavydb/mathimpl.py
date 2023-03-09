import warnings
import math
from rbc.externals import gen_codegen
from numba.core.typing.templates import ConcreteTemplate, signature, infer_global
from numba.core.types import float32, float64, int32, int64, uint64, intp
from numba.core.intrinsics import INTR_TO_CMATH
from numba.core.extending import lower_builtin as lower_cpu
from numba.cuda.mathimpl import lower as lower_gpu  # noqa: F401
# from .heavydb_compiler import heavydb_cpu_registry, heavydb_gpu_registry

# lower_cpu = heavydb_cpu_registry.lower
# lower_gpu = heavydb_gpu_registry.lower

# registry = Registry()
# infer_global = registry.register_global


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
binarys += [("fmod", "fmodf", math.fmod)]
binarys += [("hypot", "hypotf", math.hypot)]
binarys += [("remainder", "remainderf", math.remainder)]


rbc_INTR_TO_CMATH = {
    "powf": "llvm.pow.f32",
    "pow": "llvm.pow.f64",

    "sinf": "llvm.sin.f32",
    "sin": "llvm.sin.f64",

    "cosf": "llvm.cos.f32",
    "cos": "llvm.cos.f64",

    "sqrtf": "llvm.sqrt.f32",
    "sqrt": "llvm.sqrt.f64",

    "expf": "llvm.exp.f32",
    "exp": "llvm.exp.f64",

    "logf": "llvm.log.f32",
    "log": "llvm.log.f64",

    "log10f": "llvm.log10.f32",
    "log10": "llvm.log10.f64",

    "fabsf": "llvm.fabs.f32",
    "fabs": "llvm.fabs.f64",

    "floorf": "llvm.floor.f32",
    "floor": "llvm.floor.f64",

    "ceilf": "llvm.ceil.f32",
    "ceil": "llvm.ceil.f64",

    "truncf": "llvm.trunc.f32",
    "trunc": "llvm.trunc.f64",
}


if len(rbc_INTR_TO_CMATH) != len(INTR_TO_CMATH):
    warnings.warn("List of intrinsics is outdated! Please update!")


def impl_unary(fname, key, typ):
    if fname in rbc_INTR_TO_CMATH.keys():
        # use llvm intrinsics when possible
        cpu = gen_codegen(rbc_INTR_TO_CMATH.get(fname))
    else:
        cpu = gen_codegen(fname)
    # gpu = gen_codegen(f"__nv_{fname}")
    lower_cpu(key, typ)(cpu)
    # lower_gpu(key, typ)(gpu)


def impl_binary(fname, key, typ):
    if fname in rbc_INTR_TO_CMATH.keys():
        # use llvm intrinsics when possible
        cpu = gen_codegen(rbc_INTR_TO_CMATH.get(fname))
    else:
        cpu = gen_codegen(fname)
    # gpu = gen_codegen(f"__nv_{fname}")
    lower_cpu(key, typ, typ)(cpu)
    # lower_gpu(key, typ, typ)(gpu)


for fname64, fname32, key in unarys:
    impl_unary(fname64, key, float64)
    impl_unary(fname32, key, float32)


for fname64, fname32, key in binarys:
    impl_binary(fname64, key, float64)
    impl_binary(fname32, key, float32)


# manual mapping
def impl_ldexp():
    # cpu
    ldexp_cpu = gen_codegen('ldexp')
    ldexpf_cpu = gen_codegen('ldexpf')
    lower_cpu(math.ldexp, float64, int32)(ldexp_cpu)
    lower_cpu(math.ldexp, float32, int32)(ldexpf_cpu)

    # gpu
    # ldexp_gpu = gen_codegen('__nv_ldexp')
    # ldexpf_gpu = gen_codegen('__nv_ldexpf')
    # lower_gpu(math.ldexp, float64, int32)(ldexp_gpu)
    # lower_gpu(math.ldexp, float32, int32)(ldexpf_gpu)


def impl_pow():
    # cpu
    pow_cpu = gen_codegen('pow')
    powf_cpu = gen_codegen('powf')
    lower_cpu(math.pow, float64, float64)(pow_cpu)
    lower_cpu(math.pow, float32, float32)(powf_cpu)
    lower_cpu(math.pow, float64, int32)(pow_cpu)
    lower_cpu(math.pow, float32, int32)(powf_cpu)

    # gpu
    # pow_gpu = gen_codegen('__nv_pow')
    # powf_gpu = gen_codegen('__nv_powf')
    # powi_gpu = gen_codegen('__nv_powi')
    # powif_gpu = gen_codegen('__nv_powif')
    # lower_gpu(math.pow, float64, float64)(pow_gpu)
    # lower_gpu(math.pow, float32, float32)(powf_gpu)
    # lower_gpu(math.pow, float64, int32)(powi_gpu)
    # lower_gpu(math.pow, float32, int32)(powif_gpu)


impl_ldexp()
impl_pow()


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
