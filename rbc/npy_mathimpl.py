import numpy as np
from numba.targets import mathimpl, ufunc_db

# tell numba to wire np.exp2 to libm exp2.
mathimpl.unary_math_extern(np.exp2, "exp2f", "exp2")
mathimpl.unary_math_extern(np.log2, "log2f", "log2")


def np_logaddexpr_impl(context, builder, sig, args):
    def impl(x, y):
        a = np.exp(x)
        b = np.exp(y)
        return np.log(a + b)

    return context.compile_internal(builder, impl, sig, args)


def np_logaddexpr2_impl(context, builder, sig, args):
    def impl(x, y):
        a = np.power(2, x)
        b = np.power(2, y)
        return np.log2(a + b)

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
