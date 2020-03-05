import numpy as np
from numba.targets import mathimpl, ufunc_db
from numba.extending import register_jitable

# tell numba to wire np.exp2 to libm exp2.
mathimpl.unary_math_extern(np.exp2, "exp2f", "exp2")
mathimpl.unary_math_extern(np.log2, "log2f", "log2")


def np_logaddexpr_impl(context, builder, sig, args):
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


@register_jitable
def log2_1p(x):
    LOG2E = 1.442695040888963407359924681001892137  # log_2 e
    return LOG2E * np.log1p(x)


def impl(x, y):
    # return np.log2(np.power(2, x) + np.power(2, y))
    if x == y:
        return x + 1
    else:
        tmp = x - y
        if tmp > 0:
            return x + log2_1p(np.exp2(-tmp))
        elif tmp <= 0:
            return y + log2_1p(np.exp2(tmp))
        else:
            # NaN's
            return tmp


def np_logaddexpr2_impl(context, builder, sig, args):

    def impl(x, y):
        # return np.log2(np.power(2, x) + np.power(2, y))
        if x == y:
            return x + 1
        else:
            tmp = x - y
            if tmp > 0:
                return x + log2_1p(np.exp2(-tmp))
            elif tmp <= 0:
                return y + log2_1p(np.exp2(tmp))
            else:
                # NaN's
                return tmp

    return context.compile_internal(builder, impl, sig, args)


ufunc_db._lazy_init_db()

# logaddexp
ufunc_db._ufunc_db[np.logaddexp] = {
    'ff->f': np_logaddexpr_impl,
    'dd->d': np_logaddexpr_impl,
}

# logaddexp2
ufunc_db._ufunc_db[np.logaddexp2] = {
    'ff->f': np_logaddexpr2_impl,
    'dd->d': np_logaddexpr2_impl,
}
