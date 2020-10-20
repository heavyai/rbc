from numba.core.imputils import Registry
from numba.cuda import libdevice, mathimpl, libdevicefuncs
from numba.types import float32, float64, int64, uint64, int32
import numpy as np

registry = Registry()
lower = registry.lower


booleans = []
booleans += [('isnand', 'isnanf', np.isnan)]
booleans += [('isinfd', 'isinff', np.isinf)]
booleans += [('isfinited', 'finitef', np.isfinite)]
booleans += [('isnand', 'isnanf', np.isnat)]

unarys = []
# unarys += [(np.negative)]
# unarys += [(np.positive)]
# unarys += [(np.absolute)]
unarys += [('fabs', 'fabsf', np.fabs)]
# unarys += [(np.rint)]
# unarys += [(np.sign)]
# unarys += [(np.conj)]
# unarys += [(np.conjugate)]
unarys += [('exp', 'expf', np.exp)]
unarys += [('exp2', 'exp2f', np.exp2)]
unarys += [('log', 'logf', np.log)]
unarys += [('log2', 'log2f', np.log2)]
unarys += [('log10', 'log10f', np.log10)]
unarys += [('expm1', 'expm1f', np.expm1)]
unarys += [('log1p', 'log1pf', np.log1p)]
unarys += [('sqrt', 'sqrtf', np.sqrt)]
# unarys += [(np.square)]
unarys += [('cbrt', 'cbrtf', np.cbrt)]
# unarys += [(np.reciprocal)]
unarys += [('sin', 'sinf', np.sin)]
unarys += [('cos', 'cosf', np.cos)]
unarys += [('tan', 'tanf', np.tan)]
unarys += [('asin', 'asinf', np.arcsin)]
unarys += [('acos', 'acosf', np.arccos)]
unarys += [('atan', 'atanf', np.arctan)]
unarys += [('sinh', 'sinhf', np.sinh)]
unarys += [('cosh', 'coshf', np.cosh)]
unarys += [('tanh', 'tanhf', np.tanh)]
unarys += [('asinh', 'asinhf', np.arcsinh)]
unarys += [('acosh', 'acoshf', np.arccosh)]
unarys += [('atanh', 'atanhf', np.arctanh)]
# unarys += [(np.degrees)]
# unarys += [(np.radians)]
# unarys += [(np.deg2rad)]
# unarys += [(np.rad2deg)]
# unarys += [(np.invert)]
unarys += [('fabs', 'fabsf', np.fabs)]
# unarys += [('signbitd', 'signbitf', np.signbit)]
# unarys += [(np.spacing)]
unarys += [('floor', 'floorf', np.floor)]
unarys += [('ceil', 'ceilf', np.ceil)]
unarys += [('trunc', 'truncf', np.trunc)]

binarys = []
# binarys += [(np.add)]
# binarys += [(np.subtract)]
# binarys += [(np.multiply)]
# binarys += [(np.matmul)]
# binarys += [(np.divide)]
# binarys += [(np.logaddexp)]
# binarys += [(np.logaddexp2)]
# binarys += [(np.true_divide)]
# binarys += [(np.floor_divide)]
binarys += [('pow', 'powf', np.power)]
# binarys += [(np.float_power)]
# binarys += [('remainder', 'remainderf', np.remainder)]
# binarys += [('remainder', 'remainderf', np.mod)]
binarys += [('fmod', 'fmodf', np.fmod)]
# binarys += [(np.divmod)]
# binarys += [(np.heaviside)]
# binarys += [(np.gcd)]
# binarys += [(np.lcm)]
binarys += [('atan2', 'atan2f', np.arctan2)]
binarys += [('hypot', 'hypotf', np.hypot)]
# binarys += [(np.bitwise_and)]
# binarys += [(np.bitwise_or)]
# binarys += [(np.left_shift)]
# binarys += [(np.right_shift)]
# binarys += [(np.greater)]
# binarys += [(np.greater_equal)]
# binarys += [(np.less)]
# binarys += [(np.less_equal)]
# binarys += [(np.not_equal)]
# binarys += [(np.equal)]
# binarys += [(np.logical_and)]
# binarys += [(np.logical_or)]
# binarys += [(np.logical_xor)]
# binarys += [(np.maximum)]
# binarys += [(np.minimum)]
binarys += [('fmax', 'fmaxf', np.fmax)]
binarys += [('fmin', 'fminf', np.fmin)]
binarys += [('copysign', 'copysignf', np.copysign)]
binarys += [('nextafter', 'nextafterf', np.nextafter)]
binarys += [('ldexp', 'ldexpf', np.ldexp)]
binarys += [('fmod', 'fmodf', np.fmod)]

for fname64, fname32, key in booleans:
    impl32 = getattr(libdevice, fname32)
    impl64 = getattr(libdevice, fname64)
    mathimpl.impl_boolean(key, float32, impl32)
    mathimpl.impl_boolean(key, float64, impl64)

for fname64, fname32, key in unarys:
    impl32 = getattr(libdevice, fname32)
    impl64 = getattr(libdevice, fname64)
    mathimpl.impl_unary(key, float32, impl32)
    mathimpl.impl_unary(key, float64, impl64)
    mathimpl.impl_unary_int(key, int64, impl64)
    mathimpl.impl_unary_int(key, uint64, impl64)

for fname64, fname32, key in binarys:
    impl32 = getattr(libdevice, fname32)
    impl64 = getattr(libdevice, fname64)
    mathimpl.impl_binary(key, float32, impl32)
    mathimpl.impl_binary(key, float64, impl64)
    mathimpl.impl_binary_int(key, int64, impl64)
    mathimpl.impl_binary_int(key, uint64, impl64)


def impl_signbit(ty, libfunc):
    def lower_signbit_impl(context, builder, sig, args):
        signbit_sig = typing.signature(ty, ty, int32)
        libfunc_impl = context.get_function(libfunc, signbit_sig)
        return libfunc_impl(builder, args)

    lower(np.signbit, ty, int32)(lower_signbit_impl)


impl_signbit(float32, libdevice.signbitf)
impl_signbit(float64, libdevice.signbitd)


def impl_logaddexp(ty):

    def core(context, builder, sig, args):        
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

    lower(np.signbit, ty, ty)(core)


impl_logaddexp(float32)
impl_logaddexp(float64)
