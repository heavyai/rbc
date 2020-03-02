import numpy as np
from numba.targets import mathimpl, ufunc_db

# tell numba to wire np.exp2 to libm exp2.
mathimpl.unary_math_extern(np.exp2, "exp2f", "exp2")
mathimpl.unary_math_extern(np.log2, "log2f", "log2")


def np_logaddexpr_impl(context, builder, sig, args):
    # based on NumPy impl.
    # https://github.com/numpy/numpy/blob/30f985499b77381ab4748692cc76c55048ca0548/numpy/core/src/npymath/npy_math_internal.h.src#L585-L604
    def impl(x, y):
        if x == y:
            LOGE2 = 0.693147180559945309417232121458176568
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
            a = np.exp(x)
            b = np.exp(y)
            return np.log(a + b)

    return context.compile_internal(builder, impl, sig, args)


def np_logaddexpr2_impl(context, builder, sig, args):
    def impl(x, y):
        if x == y:
            return x + 1
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
