from numba.core.imputils import Registry
from numba.core import typing
from numba.cuda import libdevice, mathimpl
from numba.types import float32, float64, int64, uint64, int32, boolean
import numpy as np

registry = Registry()
lower = registry.lower

# The three lists below - booleans, unarys, binarys - maps numpy ufuncs
# to libdevice intrinsics. Some functions are commented out because either
# the CPU version works or because there is no 1:1 corresponding from the
# numpy ufunc to libdevice. For instance, there are 12 intrinsic on
# libdevice to add two numbers.

booleans = []
booleans += [('isnand', 'isnanf', np.isnan)]
booleans += [('isinfd', 'isinff', np.isinf)]
booleans += [('isfinited', 'finitef', np.isfinite)]
booleans += [('isnand', 'isnanf', np.isnat)]

unarys = []
unarys += [('fabs', 'fabsf', np.fabs)]
unarys += [('exp', 'expf', np.exp)]
unarys += [('exp2', 'exp2f', np.exp2)]
unarys += [('log', 'logf', np.log)]
unarys += [('log2', 'log2f', np.log2)]
unarys += [('log10', 'log10f', np.log10)]
unarys += [('expm1', 'expm1f', np.expm1)]
unarys += [('log1p', 'log1pf', np.log1p)]
unarys += [('sqrt', 'sqrtf', np.sqrt)]
unarys += [('cbrt', 'cbrtf', np.cbrt)]
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
unarys += [('fabs', 'fabsf', np.fabs)]
unarys += [('floor', 'floorf', np.floor)]
unarys += [('ceil', 'ceilf', np.ceil)]
unarys += [('trunc', 'truncf', np.trunc)]
# unarys += [(np.negative)]
# unarys += [(np.positive)]
# unarys += [(np.absolute)]
# unarys += [(np.rint)]
# unarys += [(np.sign)]
# unarys += [(np.conj)]
# unarys += [(np.conjugate)]
# unarys += [(np.square)]
# unarys += [(np.reciprocal)]
# unarys += [(np.degrees)]
# unarys += [(np.radians)]
# unarys += [(np.deg2rad)]
# unarys += [(np.rad2deg)]
# unarys += [(np.invert)]
# unarys += [('signbitd', 'signbitf', np.signbit)]
# unarys += [(np.spacing)]

binarys = []
binarys += [('pow', 'powf', np.power)]
binarys += [('fmod', 'fmodf', np.fmod)]
binarys += [('atan2', 'atan2f', np.arctan2)]
binarys += [('hypot', 'hypotf', np.hypot)]
binarys += [('fmax', 'fmaxf', np.fmax)]
binarys += [('fmin', 'fminf', np.fmin)]
binarys += [('copysign', 'copysignf', np.copysign)]
binarys += [('nextafter', 'nextafterf', np.nextafter)]
binarys += [('fmod', 'fmodf', np.fmod)]
# binarys += [(np.add)]
# binarys += [(np.subtract)]
# binarys += [(np.multiply)]
# binarys += [(np.matmul)]
# binarys += [(np.divide)]
# binarys += [(np.logaddexp)]
# binarys += [(np.logaddexp2)]
# binarys += [(np.true_divide)]
# binarys += [(np.floor_divide)]
# binarys += [(np.float_power)]
# binarys += [('remainder', 'remainderf', np.remainder)]
# binarys += [('remainder', 'remainderf', np.mod)]
# binarys += [(np.divmod)]
# binarys += [(np.heaviside)]
# binarys += [(np.gcd)]
# binarys += [(np.lcm)]
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


# np.signbit
def impl_signbit(ty, libfunc):
    def lower_signbit_impl(context, builder, sig, args):
        signbit_sig = typing.signature(int32, ty)  # (ty) -> int32
        libfunc_impl = context.get_function(libfunc, signbit_sig)
        result = libfunc_impl(builder, args)
        return context.cast(builder, result, int32, boolean)

    lower(np.signbit, ty)(lower_signbit_impl)


impl_signbit(float32, libdevice.signbitf)
impl_signbit(float64, libdevice.signbitd)


# np.ldexp
def impl_ldexp(ty, libfunc):
    def lower_ldexp_impl(context, builder, sig, args):
        ldexp_sig = typing.signature(ty, ty, int32)  # (ty, int32) -> ty
        libfunc_impl = context.get_function(libfunc, ldexp_sig)
        return libfunc_impl(builder, args)

    lower(np.ldexp, ty, int32)(lower_ldexp_impl)


impl_ldexp(float32, libdevice.ldexpf)
impl_ldexp(float64, libdevice.ldexp)


# np.logaddexp and np.logaddexp2
def impl_logaddexp(ty):
    from .npy_mathimpl import np_logaddexp_impl, np_logaddexp2_impl
    lower(np.logaddexp, ty, ty)(np_logaddexp_impl)
    lower(np.logaddexp2, ty, ty)(np_logaddexp2_impl)


impl_logaddexp(float32)
impl_logaddexp(float64)
