import numpy as np
from llvmlite import ir
from rbc.utils import get_version
from rbc.targetinfo import TargetInfo

if get_version("numba") >= (0, 49):
    from numba.cpython import numbers
    from numba.np import ufunc_db, npyfuncs
    from numba.core import types, funcdesc
else:
    from numba.targets import ufunc_db, npyfuncs, numbers, funcdesc
    from numba import types


def np_logaddexp_impl(context, builder, sig, args):
    # based on NumPy impl.
    # https://github.com/numpy/numpy/blob/30f985499b77381ab4748692cc76c55048ca0548/numpy/core/src/npymath/npy_math_internal.h.src#L585-L604
    def impl(x, y):
        if x == y:
            LOGE2 = 0.693147180559945309417232121458176568  # log_e 2
            return x + LOGE2
        else:
            tmp = x - y
            if tmp > 0:
                return x + np.log1p(np.exp(-tmp))
            elif tmp <= 0:
                return y + np.log1p(np.exp(tmp))
            else:
                # NaN's
                return tmp

    return context.compile_internal(builder, impl, sig, args)


def np_logaddexp2_impl(context, builder, sig, args):
    def impl(x, y):
        if x == y:
            return x + 1
        else:
            LOG2E = 1.442695040888963407359924681001892137  # log_2 e
            tmp = x - y
            if tmp > 0:
                return x + (LOG2E * np.log1p(np.exp2(-tmp)))
            elif tmp <= 0:
                return y + (LOG2E * np.log1p(np.exp2(tmp)))
            else:
                # NaN's
                return tmp

    return context.compile_internal(builder, impl, sig, args)


def np_signbit_impl(context, builder, sig, args):
    int64_t = ir.IntType(64)
    double = ir.DoubleType()
    [val] = args
    val = builder.bitcast(builder.fpext(val, double), int64_t)
    res = builder.ashr(val, int64_t(63))
    return res


def np_ldexp_impl(context, builder, sig, args):
    # check ldexp arity
    assert len(args) == 2
    assert len(sig.args) == 2

    dispatch_table = {
        types.float32: "ldexpf",
        types.float64: "ldexp",
    }

    return npyfuncs._dispatch_func_by_name_type(
        context, builder, sig, args, dispatch_table, "ldexp"
    )


def np_real_nextafter_impl(context, builder, sig, args):
    npyfuncs._check_arity_and_homogeneity(sig, args, 2)

    dispatch_table = {
        types.float32: "nextafterf",
        types.float64: "nextafter",
    }

    return npyfuncs._dispatch_func_by_name_type(
        context, builder, sig, args, dispatch_table, "nextafter"
    )


def call_external(fn_name):
    def codegen(context, builder, sig, args):
        # Need to retrieve the function name again
        fndesc = funcdesc.ExternalFunctionDescriptor(fn_name, sig.return_type, sig.args)
        func = context.declare_external_function(builder.module, fndesc)
        return builder.call(func, args)

    return codegen


def dispatch_codegen(cpu, gpu):
    def inner(context, builder, sig, args):
        impl = cpu if TargetInfo().is_cpu else gpu
        return impl(context, builder, sig, args)

    return inner


ufunc_db._lazy_init_db()

# signbit
ufunc_db._ufunc_db[np.signbit] = {
    "f->?": np_signbit_impl,
    "d->?": np_signbit_impl,
}

# logaddexp
ufunc_db._ufunc_db[np.logaddexp] = {
    "ff->f": np_logaddexp_impl,
    "dd->d": np_logaddexp_impl,
}

# logaddexp2
ufunc_db._ufunc_db[np.logaddexp2] = {
    "ff->f": np_logaddexp2_impl,
    "dd->d": np_logaddexp2_impl,
}

# nextafter
ufunc_db._ufunc_db[np.nextafter] = {
    "ff->f": dispatch_codegen(np_real_nextafter_impl, call_external("__nv_nextafterf")),
    "dd->d": dispatch_codegen(np_real_nextafter_impl, call_external("__nv_nextafter")),
}

# fabs
ufunc_db._ufunc_db[np.fabs].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_fabs_impl, call_external("__nv_fabsf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_fabs_impl, call_external("__nv_fabs")
        ),
    }
)

# arcsin
ufunc_db._ufunc_db[np.arcsin].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_asin_impl, call_external("__nv_asinf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_asin_impl, call_external("__nv_asin")
        ),
    }
)

# arccos
ufunc_db._ufunc_db[np.arccos].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_acos_impl, call_external("__nv_acosf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_acos_impl, call_external("__nv_acos")
        ),
    }
)

# arctan
ufunc_db._ufunc_db[np.arctan].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_atan_impl, call_external("__nv_atanf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_atan_impl, call_external("__nv_atan")
        ),
    }
)

# arctan2
ufunc_db._ufunc_db[np.arctan2].update(
    {
        "ff->f": dispatch_codegen(
            npyfuncs.np_real_atan2_impl, call_external("__nv_atan2f")
        ),
        "dd->d": dispatch_codegen(
            npyfuncs.np_real_atan2_impl, call_external("__nv_atan2")
        ),
    }
)

# hypot
ufunc_db._ufunc_db[np.hypot].update(
    {
        "ff->f": dispatch_codegen(
            npyfuncs.np_real_hypot_impl, call_external("__nv_hypotf")
        ),
        "dd->d": dispatch_codegen(
            npyfuncs.np_real_hypot_impl, call_external("__nv_hypot")
        ),
    }
)

# sinh
ufunc_db._ufunc_db[np.sinh].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_sinh_impl, call_external("__nv_sinhf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_sinh_impl, call_external("__nv_sinh")
        ),
    }
)

# cosh
ufunc_db._ufunc_db[np.cosh].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_cosh_impl, call_external("__nv_coshf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_cosh_impl, call_external("__nv_cosh")
        ),
    }
)

# tanh
ufunc_db._ufunc_db[np.tanh].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_tanh_impl, call_external("__nv_tanhf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_tanh_impl, call_external("__nv_tanh")
        ),
    }
)

# arcsinh
ufunc_db._ufunc_db[np.arcsinh].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_asinh_impl, call_external("__nv_asinhf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_asinh_impl, call_external("__nv_asinh")
        ),
    }
)

# arccosh
ufunc_db._ufunc_db[np.arccosh].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_acosh_impl, call_external("__nv_acoshf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_acosh_impl, call_external("__nv_acosh")
        ),
    }
)

# arctanh
ufunc_db._ufunc_db[np.arctanh].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_atanh_impl, call_external("__nv_atanhf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_atanh_impl, call_external("__nv_atanh")
        ),
    }
)

# exp
ufunc_db._ufunc_db[np.exp].update(
    {
        "f->f": dispatch_codegen(npyfuncs.np_real_exp_impl, call_external("__nv_expf")),
        "d->d": dispatch_codegen(npyfuncs.np_real_exp_impl, call_external("__nv_exp")),
    }
)

# exmp1
ufunc_db._ufunc_db[np.expm1].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_expm1_impl, call_external("__nv_expm1f")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_expm1_impl, call_external("__nv_expm1")
        ),
    }
)

# exp2
ufunc_db._ufunc_db[np.exp2].update(
    {
        "f->f": dispatch_codegen(call_external('exp2f'), call_external("__nv_exp2f")),
        "d->d": dispatch_codegen(call_external('exp2'), call_external("__nv_exp2")),
    }
)

# log
ufunc_db._ufunc_db[np.log].update(
    {
        "f->f": dispatch_codegen(npyfuncs.np_real_log_impl, call_external("__nv_logf")),
        "d->d": dispatch_codegen(npyfuncs.np_real_log_impl, call_external("__nv_log")),
    }
)

# log10
ufunc_db._ufunc_db[np.log10].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_log10_impl, call_external("__nv_log10f")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_log10_impl, call_external("__nv_log10")
        ),
    }
)

# log2
ufunc_db._ufunc_db[np.log2].update(
    {
        "f->f": dispatch_codegen(call_external('log2f'), call_external("__nv_log2f")),
        "d->d": dispatch_codegen(call_external('log2'), call_external("__nv_log2")),
    }
)

# log1p
ufunc_db._ufunc_db[np.log1p].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_log1p_impl, call_external("__nv_log1pf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_log1p_impl, call_external("__nv_log1p")
        ),
    }
)

# ldexp
ufunc_db._ufunc_db[np.ldexp] = {
    "fi->f": dispatch_codegen(np_ldexp_impl, call_external("__nv_ldexpf")),
    "fl->f": dispatch_codegen(np_ldexp_impl, call_external("__nv_ldexpf")),
    "di->d": dispatch_codegen(np_ldexp_impl, call_external("__nv_ldexp")),
    "dl->d": dispatch_codegen(np_ldexp_impl, call_external("__nv_ldexp")),
}

# floor
ufunc_db._ufunc_db[np.floor].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_floor_impl, call_external("__nv_floorf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_floor_impl, call_external("__nv_floor")
        ),
    }
)

# ceil
ufunc_db._ufunc_db[np.ceil].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_ceil_impl, call_external("__nv_ceilf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_ceil_impl, call_external("__nv_ceil")
        ),
    }
)

# trunc
ufunc_db._ufunc_db[np.trunc].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_trunc_impl, call_external("__nv_truncf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_trunc_impl, call_external("__nv_trunc")
        ),
    }
)

# rint
ufunc_db._ufunc_db[np.rint].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_rint_impl, call_external("__nv_rintf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_rint_impl, call_external("__nv_rint")
        ),
    }
)

# copysign
ufunc_db._ufunc_db[np.copysign].update(
    {
        "ff->f": dispatch_codegen(
            npyfuncs.np_real_copysign_impl, call_external("__nv_copysignf")
        ),
        "dd->d": dispatch_codegen(
            npyfuncs.np_real_copysign_impl, call_external("__nv_copysign")
        ),
    }
)

# power
ufunc_db._ufunc_db[np.power].update(
    {
        "ff->f": dispatch_codegen(numbers.real_power_impl, call_external("__nv_powf")),
        "dd->d": dispatch_codegen(numbers.real_power_impl, call_external("__nv_pow")),
    }
)

# sqrt
ufunc_db._ufunc_db[np.sqrt].update(
    {
        "f->f": dispatch_codegen(
            npyfuncs.np_real_sqrt_impl, call_external("__nv_sqrtf")
        ),
        "d->d": dispatch_codegen(
            npyfuncs.np_real_sqrt_impl, call_external("__nv_sqrt")
        ),
    }
)

# sin
ufunc_db._ufunc_db[np.sin].update(
    {
        "f->f": dispatch_codegen(npyfuncs.np_real_sin_impl, call_external("__nv_sinf")),
        "d->d": dispatch_codegen(npyfuncs.np_real_sin_impl, call_external("__nv_sin")),
    }
)

# cos
ufunc_db._ufunc_db[np.cos].update(
    {
        "f->f": dispatch_codegen(npyfuncs.np_real_cos_impl, call_external("__nv_cosf")),
        "d->d": dispatch_codegen(npyfuncs.np_real_cos_impl, call_external("__nv_cos")),
    }
)

# tan
ufunc_db._ufunc_db[np.tan].update(
    {
        "f->f": dispatch_codegen(npyfuncs.np_real_tan_impl, call_external("__nv_tanf")),
        "d->d": dispatch_codegen(npyfuncs.np_real_tan_impl, call_external("__nv_tan")),
    }
)
