"""Collections of library function names.
"""


class Library:
    """Base class for a collection of library function names.
    """

    @staticmethod
    def get(libname, _cache={}):
        if libname in _cache:
            return _cache[libname]
        if libname == 'stdlib':
            r = Stdlib()
        elif libname == 'stdio':
            r = Stdio()
        elif libname == 'm':
            r = Mlib()
        elif libname == 'libdevice':
            r = Libdevice()
        elif libname == 'nvvm':
            r = NVVMIntrinsics()
        elif libname == 'llvm':
            r = LLVMIntrinsics()
        elif libname == 'omniscidb':
            r = OmnisciDB()
        else:
            raise ValueError(f'Unknown library {libname}')
        _cache[libname] = r
        return r

    def __contains__(self, fname):
        return self.check(fname)

    def check(self, fname):
        """
        Return True if library contains a function with given name.
        """
        return fname in self._function_names


class OmnisciDB(Library):

    name = 'omniscidb'

    _function_names = list('''
    allocate_varlen_buffer
    '''.strip().split())


class Stdlib(Library):
    """
    Reference: http://www.cplusplus.com/reference/cstdlib/
    """
    name = 'stdlib'
    _function_names = list(''' atof atoi atol atoll strtod strtof strtol strtold strtoll strtoul
    strtoull rand srand calloc free malloc realloc abort atexit
    at_quick_exit exit getenv quick_exit system bsearch qsort abs div
    labs ldiv llabs lldiv mblen mbtowc wctomb mbstowcs wcstombs '''.strip().split())


class Stdio(Library):
    """
    Reference: http://www.cplusplus.com/reference/cstdio/
    """
    name = 'stdio'

    _function_names = list(''' remove rename tmpfile tmpnam fclose fflush fopen freopen setbuf
    setvbuf fprintf fscanf printf scanf snprintf sprintf sscanf
    vfprintf vfscanf vprintf vscanf vsnprintf vsprintf vsscanf fgetc
    fgets fputc fputs getc getchar gets putc putchar puts ungetc fread
    fwrite fgetpos fseek fsetpos ftell rewind clearerr feof ferror
    perror '''.strip().split())


class Mlib(Library):
    """
    References:
      https://www.gnu.org/software/libc/manual/html_node/Mathematics.html
      https://en.cppreference.com/w/cpp/header/cmath
    """
    name = 'm'

    _function_names = list('''sin sinf sinl cos cosf cosl tan tanf tanl sincos sincosf sincosl
    csin csinf csinl ccos ccosf ccosl ctan ctanf ctanl asin asinf
    asinl acos acosf acosl atan atanf atanl atan2 atan2f atan2l casin
    casinf casinl cacos cacosf cacosl catan catanf catanl exp expf
    expl exp2 exp2f exp2l exp10 exp10f exp10l log logf logl log2 log2f
    log2l log10 log10f log10l logb logbf logbl ilogb ilogbf ilogbl pow
    powf powl sqrt sqrtf sqrtl cbrt cbrtf cbrtl hypot hypotf hypotl
    expm1 expm1f expm1l log1p log1pf log1pl clog clogf clogl clog10
    clog10f clog10l csqrt csqrtf csqrtl cpow cpowf cpowl sinh sinhf
    sinhl cosh coshf coshl tanh tanhf tanhl csinh csinhf csinhl ccosh
    ccoshf ccoshl ctanh ctanhf ctanhl asinh asinhf asinhl acosh acoshf
    acoshl atanh atanhf atanhl casinh casinhf casinhl cacosh cacoshf
    cacoshl catanh catanhf catanhl erf erff erfl erfc erfcf erfcl
    lgamma lgammaf lgammal tgamma tgammaf tgammal lgamma_r lgammaf_r
    lgammal_r gamma gammaf gammal j0 j0f j0l j1 j1f j1l jn jnf jnl y0
    y0f y0l y1 y1f y1l yn ynf ynl rand srand rand_r random srandom
    initstate setstate random_r srandom_r initstate_r setstate_r
    drand48 erand48 lrand48 nrand48 mrand48 jrand48 srand48 seed48
    lcong48 drand48_r erand48_r lrand48_r nrand48_r mrand48_r
    jrand48_r srand48_r seed48_r lcong48_r abs labs llabs fabs fabsf
    fabsl cabs cabsf cabsl frexp frexpf frexpl ldexp ldexpf ldexpl
    scalb scalbf scalbl scalbn scalbnf scalbnl significand
    significandf significandl ceil ceilf ceill floor floorf floorl
    trunc truncf truncl rint rintf rintl nearbyint nearbyintf
    nearbyintl round roundf roundl roundeven roundevenf roundevenl
    lrint lrintf lrintl lround lroundf lroundl llround llroundf
    llroundl fromfp fromfpf fromfpl ufromfp ufromfpf ufromfpl fromfpx
    fromfpxf fromfpxl ufromfpx ufromfpxf ufromfpxl modf modff modfl
    fmod fmodf fmodl remainder remainderf remainderl drem dremf dreml
    copysign copysignf copysignl signbit signbitf signbitl nextafter
    nextafterf nextafterl nexttoward nexttowardf nexttowardl nextup
    nextupf nextupl nextdown nextdownf nextdownl nan nanf nanl
    canonicalize canonicalizef canonicalizel getpayload getpayloadf
    getpayloadl setpayload setpayloadf setpayloadl setpayloadsig
    setpayloadsigf setpayloadsigl isgreater isgreaterequal isless
    islessequal islessgreater isunordered iseqsig totalorder
    totalorderf totalorderl totalordermag totalorderf totalorderl fmin
    fminf fminl fmax fmaxf fmaxl fminmag fminmagf fminmagl fmaxmag
    fmaxmagf fmaxmagl fdim fdimf fdiml fma fmaf fmal fadd faddf faddl
    fsub fsubf fsubl fmul fmulf fmull fdiv fdivf fdivl '''.strip().split())


def drop_suffix(f):
    s = f.rsplit('.', 1)[-1]
    if s in ['p0i8', 'f64', 'f32', 'i1', 'i8', 'i16', 'i32', 'i64', 'i128']:
        f = f[:-len(s)-1]
        return drop_suffix(f)
    return f


def get_llvm_name(f, prefix='llvm.'):
    """Return normalized name of a llvm intrinsic name.
    """
    if f.startswith(prefix):
        return drop_suffix(f[len(prefix):])
    return f


class LLVMIntrinsics(Library):
    """LLVM intrinsic function names with prefix `llvm.` removed.

    Reference: https://llvm.org/docs/LangRef.html#intrinsic-functions
    """
    name = 'llvm'

    def check(self, fname):
        if fname.startswith('llvm.'):
            return Library.check(self, get_llvm_name(fname))
        return False

    _function_names = list(''' va_start va_end va_copy gcroot gcread gcwrite returnaddress
    addressofreturnaddress sponentry frameaddress stacksave
    stackrestore get.dynamic.area.offset prefetch pcmarker
    readcyclecounter clear_cache instrprof.increment
    instrprof.increment.step instrprof.value.profile thread.pointer
    call.preallocated.setup call.preallocated.arg
    call.preallocated.teardown abs smax smin umax umin memcpy
    memcpy.inline memmove sqrt powi sin cos pow exp exp2 log log10
    log2 fma fabs minnum maxnum minimum maximum copysign floor ceil
    trunc rint nearbyint round roundeven lround llround lrint llrint
    ctpop ctlz cttz fshl fshr sadd.with.overflow uadd.with.overflow
    ssub.with.overflow usub.with.overflow smul.with.overflow
    umul.with.overflow sadd.sat uadd.sat ssub.sat usub.sat sshl.sat
    ushl.sat smul.fix umul.fix smul.fix.sat umul.fix.sat sdiv.fix
    udiv.fix sdiv.fix.sat udiv.fix.sat canonicalize fmuladd
    set.loop.iterations test.set.loop.iterations loop.decrement.reg
    loop.decrement vector.reduce.add vector.reduce.fadd
    vector.reduce.mul vector.reduce.fmul vector.reduce.and
    vector.reduce.or vector.reduce.xor vector.reduce.smax
    vector.reduce.smin vector.reduce.umax vector.reduce.umin
    vector.reduce.fmax vector.reduce.fmin matrix.transpose
    matrix.multiply matrix.column.major.load matrix.column.major.store
    convert.to.fp16 convert.from.fp16 init.trampoline
    adjust.trampoline lifetime.start lifetime.end invariant.start
    invariant.end launder.invariant.group strip.invariant.group
    experimental.constrained.fadd experimental.constrained.fsub
    experimental.constrained.fmul experimental.constrained.fdiv
    experimental.constrained.frem experimental.constrained.fma
    experimental.constrained.fptoui experimental.constrained.fptosi
    experimental.constrained.uitofp experimental.constrained.sitofp
    experimental.constrained.fptrunc experimental.constrained.fpext
    experimental.constrained.fmuladd experimental.constrained.sqrt
    experimental.constrained.pow experimental.constrained.powi
    experimental.constrained.sin experimental.constrained.cos
    experimental.constrained.exp experimental.constrained.exp2
    experimental.constrained.log experimental.constrained.log10
    experimental.constrained.log2 experimental.constrained.rint
    experimental.constrained.lrint experimental.constrained.llrint
    experimental.constrained.nearbyint experimental.constrained.maxnum
    experimental.constrained.minnum experimental.constrained.maximum
    experimental.constrained.minimum experimental.constrained.ceil
    experimental.constrained.floor experimental.constrained.round
    experimental.constrained.roundeven experimental.constrained.lround
    experimental.constrained.llround experimental.constrained.trunc
    flt.rounds var.annotation ptr.annotation annotation
    codeview.annotation trap debugtrap stackprotector stackguard
    objectsize expect expect.with.probability assume ssa_copy
    type.test type.checked.load donothing experimental.deoptimize
    experimental.guard experimental.widenable.condition load.relative
    sideeffect is.constant ptrmask vscale
    memcpy.element.unordered.atomic memmove.element.unordered.atomic
    memset.element.unordered.atomic objc.autorelease
    objc.autoreleasePoolPop objc.autoreleasePoolPush
    objc.autoreleaseReturnValue objc.copyWeak objc.destroyWeak
    objc.initWeak objc.loadWeak objc.loadWeakRetained objc.moveWeak
    objc.release objc.retain objc.retainAutorelease
    objc.retainAutoreleaseReturnValue
    objc.retainAutoreleasedReturnValue objc.retainBlock
    objc.storeStrong objc.storeWeak preserve.array.access.index
    preserve.union.access.index preserve.struct.access.index '''.strip().split())


class NVVMIntrinsics(Library):
    """NVVM intrinsic function names with prefix `llvm.` removed.

    Reference: https://docs.nvidia.com/cuda/nvvm-ir-spec/index.html#intrinsic-functions
    """
    name = 'nvvm'

    def check(self, fname):
        if fname.startswith('llvm.'):
            return Library.check(self, get_llvm_name(fname))
        return False

    _function_names = list(''' memcpy memmove memset sqrt fma bswap ctpop ctlz cttz fmuladd
    convert.to.fp16.f32 convert.from.fp16.f32 convert.to.fp16
    convert.from.fp16 lifetime.start lifetime.end invariant.start
    invariant.end var.annotation ptr.annotation annotation expect
    donothing '''.strip().split())


class Libdevice(Library):
    """NVIDIA libdevice function names with prefix `__nv_` removed.

    Reference: https://docs.nvidia.com/cuda/libdevice-users-guide/function-desc.html#function-desc
    """
    name = 'libdevice'

    def check(self, fname):
        if fname.startswith('__nv_'):
            return Library.check(self, get_llvm_name(fname, prefix='__nv_'))
        return False

    _function_names = list(''' abs acos acosf acosh acoshf asin asinf asinh asinhf atan atan2
    atan2f atanf atanh atanhf brev brevll byte_perm cbrt cbrtf ceil
    ceilf clz clzll copysign copysignf cos cosf cosh coshf cospi
    cospif dadd_rd dadd_rn dadd_ru dadd_rz ddiv_rd ddiv_rn ddiv_ru
    ddiv_rz dmul_rd dmul_rn dmul_ru dmul_rz double2float_rd
    double2float_rn double2float_ru double2float_rz double2hiint
    double2int_rd double2int_rn double2int_ru double2int_rz
    double2ll_rd double2ll_rn double2ll_ru double2ll_rz double2loint
    double2uint_rd double2uint_rn double2uint_ru double2uint_rz
    double2ull_rd double2ull_rn double2ull_ru double2ull_rz
    double_as_longlong drcp_rd drcp_rn drcp_ru drcp_rz dsqrt_rd
    dsqrt_rn dsqrt_ru dsqrt_rz erf erfc erfcf erfcinv erfcinvf erfcx
    erfcxf erff erfinv erfinvf exp exp10 exp10f exp2 exp2f expf expm1
    expm1f fabs fabsf fadd_rd fadd_rn fadd_ru fadd_rz fast_cosf
    fast_exp10f fast_expf fast_fdividef fast_log10f fast_log2f
    fast_logf fast_powf fast_sincosf fast_sinf fast_tanf fdim fdimf
    fdiv_rd fdiv_rn fdiv_ru fdiv_rz ffs ffsll finitef float2half_rn
    float2int_rd float2int_rn float2int_ru float2int_rz float2ll_rd
    float2ll_rn float2ll_ru float2ll_rz float2uint_rd float2uint_rn
    float2uint_ru float2uint_rz float2ull_rd float2ull_rn float2ull_ru
    float2ull_rz float_as_int floor floorf fma fma_rd fma_rn fma_ru
    fma_rz fmaf fmaf_rd fmaf_rn fmaf_ru fmaf_rz fmax fmaxf fmin fminf
    fmod fmodf fmul_rd fmul_rn fmul_ru fmul_rz frcp_rd frcp_rn frcp_ru
    frcp_rz frexp frexpf frsqrt_rn fsqrt_rd fsqrt_rn fsqrt_ru fsqrt_rz
    fsub_rd fsub_rn fsub_ru fsub_rz hadd half2float hiloint2double
    hypot hypotf ilogb ilogbf int2double_rn int2float_rd int2float_rn
    int2float_ru int2float_rz int_as_float isfinited isinfd isinff
    isnand isnanf j0 j0f j1 j1f jn jnf ldexp ldexpf lgamma lgammaf
    ll2double_rd ll2double_rn ll2double_ru ll2double_rz ll2float_rd
    ll2float_rn ll2float_ru ll2float_rz llabs llmax llmin llrint
    llrintf llround llroundf log log10 log10f log1p log1pf log2 log2f
    logb logbf logf longlong_as_double max min modf modff mul24
    mul64hi mulhi nan nanf nearbyint nearbyintf nextafter nextafterf
    normcdf normcdff normcdfinv normcdfinvf popc popcll pow powf powi
    powif rcbrt rcbrtf remainder remainderf remquo remquof rhadd rint
    rintf round roundf rsqrt rsqrtf sad saturatef scalbn scalbnf
    signbitd signbitf sin sincos sincosf sincospi sincospif sinf sinh
    sinhf sinpi sinpif sqrt sqrtf tan tanf tanh tanhf tgamma tgammaf
    trunc truncf uhadd uint2double_rn uint2float_rd uint2float_rn
    uint2float_ru uint2float_rz ull2double_rd ull2double_rn
    ull2double_ru ull2double_rz ull2float_rd ull2float_rn ull2float_ru
    ull2float_rz ullmax ullmin umax umin umul24 umul64hi umulhi urhadd
    usad y0 y0f y1 y1f yn ynf '''.strip().split())
