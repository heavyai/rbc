# Author: Pearu Peterson
# Created: February 2019

import re
from llvmlite import ir
import llvmlite.binding as llvm
from .targetinfo import TargetInfo
from typing import List, Set
from .utils import get_version
from .errors import OmnisciUnsupportedError

if get_version('numba') >= (0, 49):
    from numba.core import codegen, cpu, compiler_lock, \
        registry, typing, compiler, sigutils, cgutils, \
        extending
    from numba.core import types as nb_types
    from numba.core import errors as nb_errors
else:
    from numba.targets import codegen, cpu, registry
    from numba import compiler_lock, typing, compiler, \
        sigutils, cgutils, extending
    from numba import types as nb_types
    from numba import errors as nb_errors

int32_t = ir.IntType(32)


def _lf(lst):
    return lst + [e + 'f' for e in lst] + [e + 'l' for e in lst]


def _f(lst):
    return lst + [e + 'f' for e in lst]


exp_funcs = ['exp', 'exp2', 'expm1', 'log', 'log2', 'log10',
             'log1p', 'ilogb', 'logb']
power_funcs = ['sqrt', 'cbrt', 'hypot', 'pow']
trigonometric_funcs = ['sin', 'cos', 'tan', 'asin', 'acos', 'atan', 'atan2']
hyperbolic_funcs = ['sinh', 'cosh', 'tanh', 'asinh', 'acosh', 'atanh']
nearest_funcs = ['ceil', 'floor', 'trunc', 'round', 'lround', 'llround',
                 'nearbyint', 'rint', 'lrint', 'llrint']
fp_funcs = ['frexp', 'ldexp', 'modf', 'scalbn', 'scalbln', 'nextafter',
            'nexttoward', 'spacing']
classification_funcs = ['fpclassify', 'isfinite', 'isinf', 'isnan',
                        'isnormal', 'signbit']

pymath_funcs = _f(['erf', 'erfc', 'lgamma', 'tgamma'])

fp_funcs = _lf([*exp_funcs, *power_funcs, *trigonometric_funcs,
                *hyperbolic_funcs, *nearest_funcs, *fp_funcs])

libm_funcs = [*fp_funcs, *classification_funcs]

stdio_funcs = ['printf', 'puts', 'fflush']

stdlib_funcs = ['free', 'calloc']


def get_called_functions(library, funcname):
    module = library._final_module
    func = module.get_function(funcname)
    assert func.name == funcname, (func.name, funcname)
    intrinsics, declarations, defined, libraries = set(), set(), set(), set()
    for block in func.blocks:
        for instruction in block.instructions:
            if instruction.opcode == 'call':
                name = list(instruction.operands)[-1].name
                f = module.get_function(name)
                if name.startswith('llvm.'):
                    intrinsics.add(name)
                elif f.is_declaration:
                    found = False
                    for lib in library._linking_libraries:
                        for df in lib.get_defined_functions():
                            if name == df.name:
                                defined.add(name)
                                libraries.add(lib)
                                found = True
                                result = get_called_functions(lib, name)
                                intrinsics.update(result[0])
                                declarations.update(result[1])
                                defined.update(result[2])
                                libraries.update(result[3])
                                break
                        if found:
                            break
                    if not found:
                        declarations.add(name)
                else:
                    result = get_called_functions(library, name)
                    intrinsics.update(result[0])
                    declarations.update(result[1])
                    defined.update(result[2])
                    libraries.update(result[3])
    return intrinsics, declarations, defined, libraries


def get_function_dependencies(module, funcname, _deps=None):
    if _deps is None:
        _deps = dict()
    func = module.get_function(funcname)
    assert func.name == funcname, (func.name, funcname)
    for block in func.blocks:
        for instruction in block.instructions:
            if instruction.opcode == 'call':
                name = list(instruction.operands)[-1].name
                f = module.get_function(name)
                if name.startswith('llvm.'):
                    _deps[name] = 'intrinsic'
                elif f.is_declaration:
                    if name in libm_funcs:
                        _deps[name] = 'libm'
                    elif name in stdio_funcs:
                        _deps[name] = 'stdio'
                    elif name in stdlib_funcs:
                        _deps[name] = 'stdlib'
                    elif name in ['allocate_varlen_buffer']:
                        _deps[name] = 'omnisci_internal'
                    elif name in pymath_funcs:
                        _deps[name] = 'pymath'
                    elif name.startswith('__nv'):
                        _deps[name] = 'cuda'
                    else:
                        _deps[name] = 'undefined'
                else:
                    if name not in _deps:
                        _deps[name] = 'defined'
                        get_function_dependencies(module, name, _deps=_deps)
    return _deps


def catch_undefined_functions(main_module: llvm.ModuleRef, target: TargetInfo,
                              function_names: List[str]) -> Set[str]:
    used_functions = set(function_names)
    for fname in function_names:
        deps = get_function_dependencies(main_module, fname)
        for fn_name, descr in deps.items():
            used_functions.add(fn_name)
            if descr == 'undefined':
                if fn_name.startswith('numba_') and target.has_numba:
                    continue
                if fn_name.startswith('Py') and target.has_cpython:
                    continue
                if fn_name.startswith('npy_') and target.has_numpy:
                    continue
                raise RuntimeError('function `%s` is undefined' % (fn_name))
    return used_functions


# ---------------------------------------------------------------------------
# CPU Context classes
class JITRemoteCPUCodegen(codegen.JITCPUCodegen):
    # TODO: introduce JITRemoteCodeLibrary?
    _library_class = codegen.JITCodeLibrary

    def __init__(self, name, target_info):
        self.target_info = target_info
        super(JITRemoteCPUCodegen, self).__init__(name)

    def _get_host_cpu_name(self):
        return self.target_info.device_name

    def _get_host_cpu_features(self):
        features = self.target_info.device_features
        if llvm.llvm_version_info[0] < 9:
            # See https://github.com/xnd-project/rbc/issues/45
            for f in ['cx8', 'enqcmd', 'avx512bf16']:
                features = features.replace('+' + f, '').replace('-' + f, '')
        return features

    def _customize_tm_options(self, options):
        super(JITRemoteCPUCodegen, self)._customize_tm_options(options)
        # fix reloc_model as the base method sets it using local target
        if self.target_info.arch.startswith('x86'):
            reloc_model = 'static'
        else:
            reloc_model = 'default'
        options['reloc'] = reloc_model


class RemoteCPUContext(cpu.CPUContext):

    def __init__(self, typing_context, target_info):
        self.target_info = target_info
        super(RemoteCPUContext, self).__init__(typing_context)

    @compiler_lock.global_compiler_lock
    def init(self):
        self.address_size = self.target_info.bits
        self.is32bit = (self.address_size == 32)
        self._internal_codegen = JITRemoteCPUCodegen("numba.exec",
                                                     self.target_info)

        # Map external C functions.
        # nb.core.externals.c_math_functions.install(self)
        # TODO, seems problematic only for 32bit cases

        # Initialize NRT runtime
        # nb.core.argets.cpu.rtsys.initialize(self)
        # TODO: is this needed for LLVM IR generation?

        # import numba.unicode  # unicode support is not relevant here

    # TODO: overwrite load_additional_registries, call_conv?, post_lowering

# ---------------------------------------------------------------------------
# GPU Context classes


class RemoteGPUTargetContext(cpu.CPUContext):

    def __init__(self, typing_context, target_info):
        self.target_info = target_info
        super().__init__(typing_context)

    @compiler_lock.global_compiler_lock
    def init(self):
        self.address_size = self.target_info.bits
        self.is32bit = (self.address_size == 32)
        self._internal_codegen = JITRemoteCPUCodegen("numba.exec",
                                                     self.target_info)

    def load_additional_registries(self):
        # libdevice and math from cuda have precedence over the ones from CPU
        if get_version('numba') >= (0, 52):
            from numba.cuda import libdeviceimpl, mathimpl
            from .omnisci_backend import cuda_npyimpl
            self.install_registry(libdeviceimpl.registry)
            self.install_registry(mathimpl.registry)
            self.install_registry(cuda_npyimpl.registry)
        else:
            import warnings
            warnings.warn("libdevice bindings requires Numba 0.52 or newer,"
                          f" got Numba v{'.'.join(map(str, get_version('numba')))}")
        super().load_additional_registries()


class RemoteGPUTypingContext(typing.Context):
    def load_additional_registries(self):
        if get_version('numba') >= (0, 52):
            from numba.core.typing import npydecl
            from numba.cuda import cudamath, libdevicedecl
            self.install_registry(npydecl.registry)
            self.install_registry(cudamath.registry)
            self.install_registry(libdevicedecl.registry)
        super().load_additional_registries()

# ---------------------------------------------------------------------------
# Code generation methods


def make_wrapper(fname, atypes, rtype, cres, verbose=False):
    """Make wrapper function to numba compile result.

    The compilation result contains a function with prototype::

      <status> <function name>(<rtype>** result, <arguments>)

    The make_wrapper adds a wrapper function to compilation result
    library with the following prototype::

      <rtype> <fname>(<arguments>)

    or, if <rtype> is Omnisci Array, then

      void <fname>(<rtype>* <arguments>)

    Parameters
    ----------
    fname : str
      Function name.
    atypes : tuple
      A tuple of argument Numba types.
    rtype : numba.Type
      The return Numba type
    cres : CompileResult
      Numba compilation result.
    verbose: bool
      When True, insert printf statements for debugging errors. Use
      False for CUDA target.

    """
    fndesc = cres.fndesc
    module = cres.library.create_ir_module(fndesc.unique_name)
    context = cres.target_context
    ll_argtypes = [context.get_value_type(ty) for ty in atypes]
    ll_return_type = context.get_value_type(rtype)

    # TODO: design a API for custom wrapping
    if type(rtype).__name__ == 'ArrayPointer':
        wrapty = ir.FunctionType(ir.VoidType(),
                                 [ll_return_type] + ll_argtypes)
        wrapfn = module.add_function(wrapty, fname)
        builder = ir.IRBuilder(wrapfn.append_basic_block('entry'))
        fnty = context.call_conv.get_function_type(rtype, atypes)
        fn = builder.module.add_function(fnty, cres.fndesc.llvm_func_name)
        status, out = context.call_conv.call_function(
            builder, fn, rtype, atypes, wrapfn.args[1:])
        with cgutils.if_unlikely(builder, status.is_error):
            if verbose:
                cgutils.printf(builder,
                               f"rbc: {fname} failed with status code %i\n",
                               status.code)
            builder.ret_void()
        builder.store(builder.load(out), wrapfn.args[0])
        builder.ret_void()
    else:
        wrapty = ir.FunctionType(ll_return_type, ll_argtypes)
        wrapfn = module.add_function(wrapty, fname)
        builder = ir.IRBuilder(wrapfn.append_basic_block('entry'))
        fnty = context.call_conv.get_function_type(rtype, atypes)
        fn = builder.module.add_function(fnty, cres.fndesc.llvm_func_name)
        status, out = context.call_conv.call_function(
            builder, fn, rtype, atypes, wrapfn.args)
        if verbose:
            with cgutils.if_unlikely(builder, status.is_error):
                cgutils.printf(builder,
                               f"rbc: {fname} failed with status code %i\n",
                               status.code)
        builder.ret(out)

    cres.library.add_ir_module(module)


def compile_to_LLVM(functions_and_signatures,
                    target: TargetInfo,
                    pipeline_class=compiler.Compiler,
                    debug=False):
    """Compile functions with given signatures to target specific LLVM IR.

    Parameters
    ----------
    functions_and_signatures : list
      Specify a list of Python function and its signatures pairs.
    target : TargetInfo
      Specify target device information.
    debug : bool

    Returns
    -------
    module : llvmlite.binding.ModuleRef
      LLVM module instance. To get the IR string, use `str(module)`.

    """

    target_desc = registry.cpu_target

    if target is None:
        # RemoteJIT
        target = TargetInfo.host()
        typing_context = target_desc.typing_context
        target_context = target_desc.target_context
    else:
        # OmnisciDB target
        if target.is_cpu:
            typing_context = typing.Context()
            target_context = RemoteCPUContext(typing_context, target)
        else:  # gpu
            typing_context = RemoteGPUTypingContext()
            target_context = RemoteGPUTargetContext(typing_context, target)

        # Bring over Array overloads (a hack):
        target_context._defns = target_desc.target_context._defns
        # Fixes rbc issue 74:
        target_desc.typing_context.target_info = target
        target_desc.target_context.target_info = target

    typing_context.target_info = target
    target_context.target_info = target

    codegen = target_context.codegen()
    main_library = codegen.create_library('rbc.irtools.compile_to_IR')
    main_module = main_library._final_module

    flags = compiler.Flags()
    flags.set('no_compile')
    flags.set('no_cpython_wrapper')
    if get_version('numba') >= (0, 49):
        flags.set('no_cfunc_wrapper')

    function_names = []
    succesful_fids = []
    required_libraries = set()
    for func, signatures in functions_and_signatures:
        for fid, sig in signatures.items():
            fname = func.__name__ + sig.mangling()
            args, return_type = sigutils.normalize_signature(
                sig.tonumba(bool_is_int8=True))
            try:
                cres = compiler.compile_extra(typingctx=typing_context,
                                              targetctx=target_context,
                                              func=func,
                                              args=args,
                                              return_type=return_type,
                                              flags=flags,
                                              library=main_library,
                                              locals={},
                                              pipeline_class=pipeline_class)
            except OmnisciUnsupportedError as msg:
                for m in re.finditer(r'[|]OmnisciUnsupportedError[|](.*?)\n', str(msg), re.S):
                    print(f'Skipping {fname} `{sig}`: {m.group(0)[25:]}')
                continue
            except nb_errors.TypingError as msg:
                for m in re.finditer(r'[|]OmnisciUnsupportedError[|](.*?)\n', str(msg), re.S):
                    print(f'Skipping {fname} `{sig}`: {m.group(0)[25:]}')
                    break
                else:
                    raise
                continue
            except Exception:
                raise

            skip = False
            intrinsics, declarations, defined, libraries = get_called_functions(
                cres.library, cres.fndesc.llvm_func_name)
            for f in declarations:
                if target.is_gpu:
                    if f.startswith('__nv_') and f[5:] in get_libdevice_functions():
                        # numba supports libdevice functions starting from 0.52
                        continue

                if target.is_cpu:
                    if f in get_libm_functions():
                        # omniscidb is linked against m library
                        continue
                    if f in get_stdio_functions():
                        # omniscidb is linked against stdio library
                        continue
                    if f in get_stdlib_functions():
                        # omniscidb is linked against stdlib library
                        continue

                print(f'Skipping {fname} for {target.name} that uses unknown function `{f}`')
                skip = True
                break

            for f in intrinsics:
                if target.is_gpu:
                    nf = get_llvm_name(f)
                    if nf in get_nvvm_intrinsic_functions():
                        continue
                if target.is_cpu:
                    nf = get_llvm_name(f)
                    if nf in get_llvm_intrinsic_functions():
                        continue
                print(f'Skipping {fname} for {target.name} that uses unsupported intrinsic `{f}`')
                skip = True
                break

            if skip:
                continue

            function_names.append(fname)
            succesful_fids.append(fid)
            make_wrapper(fname, args, return_type, cres,
                         verbose=debug)
            required_libraries.update(libraries)

    for lib in required_libraries:
        main_module.link_in(
            lib._get_module_for_linking(), preserve=True,
        )

    main_library._optimize_final_module()

    # Catch undefined functions:
    used_functions = catch_undefined_functions(main_module, target, function_names)

    unused_functions = [f.name for f in main_module.functions
                        if f.name not in used_functions]

    if debug:
        print('compile_to_IR: the following functions are used')
        for fname in used_functions:
            lf = main_module.get_function(fname)
            print('  [ALIVE]', fname, 'with', lf.linkage)

    if unused_functions:
        if debug:
            print('compile_to_IR: the following functions are not used'
                  ' and will be removed:')
        for fname in unused_functions:
            lf = main_module.get_function(fname)
            if lf.is_declaration:
                # if the function is a declaration,
                # we just put the linkage as external
                lf.linkage = llvm.Linkage.external
            else:
                # but if the function is not a declaration,
                # we change the linkage to private
                lf.linkage = llvm.Linkage.private
            if debug:
                print('  [DEAD]', fname, 'with', lf.linkage)

        main_library._optimize_final_module()
    # TODO: determine unused global_variables and struct_types

    main_module.verify()
    main_library._finalized = True

    main_module.triple = target.triple
    main_module.data_layout = target.datalayout

    return main_module, succesful_fids


def compile_IR(ir):
    """Return execution engine with IR compiled in.

    Parameters
    ----------
    ir : str
      Specify LLVM IR code as a string.

    Returns
    -------
    engine :
      Execution engine.


    Examples
    --------

        To get the address of the compiled functions, use::

          addr = engine.get_function_address("<function name>")
    """
    triple = re.search(
        r'target\s+triple\s*=\s*"(?P<triple>[-\d\w\W_]+)"\s*$',
        ir, re.M).group('triple')

    # Create execution engine
    target = llvm.Target.from_triple(triple)
    target_machine = target.create_target_machine()
    backing_mod = llvm.parse_assembly("")
    engine = llvm.create_mcjit_compiler(backing_mod, target_machine)

    # Create LLVM module and compile
    mod = llvm.parse_assembly(ir)
    mod.verify()
    engine.add_module(mod)
    engine.finalize_object()
    engine.run_static_constructors()

    return engine


def cg_fflush(builder):
    int8_t = ir.IntType(8)
    fflush_fnty = ir.FunctionType(int32_t, [int8_t.as_pointer()])
    fflush_fn = builder.module.get_or_insert_function(
        fflush_fnty, name='fflush')

    builder.call(fflush_fn, [int8_t.as_pointer()(None)])


@extending.intrinsic
def fflush(typingctx):
    """fflush that can be called from Numba jit-decorated functions.

    Note: fflush is available only for CPU target.
    """
    sig = nb_types.void(nb_types.void)

    def codegen(context, builder, signature, args):
        if typingctx.target_info.is_cpu:
            cg_fflush(builder)

    return sig, codegen


@extending.intrinsic
def printf(typingctx, format_type, *args):
    """printf that can be called from Numba jit-decorated functions.

    Note: printf is available only for CPU target.
    """
    if isinstance(format_type, nb_types.StringLiteral):
        sig = nb_types.void(format_type, nb_types.BaseTuple.from_types(args))

        def codegen(context, builder, signature, args):
            if typingctx.target_info.is_cpu:
                cgutils.printf(builder, format_type.literal_value, *args[1:])
                cg_fflush(builder)

        return sig, codegen


def get_llvm_name(f):
    """Return normalized name of a llvm intrinsic name.
    """
    if f.startswith('llvm.'):
        nf = f[5:]
        p = nf.rsplit('.', 1)[-1]
        if p in ['p0i8',
                 'f64', 'f32',
                 'i1', 'i8', 'i16', 'i32', 'i64', 'i128']:
            # TODO: implement more robust suffix dropper
            nf = nf[:-len(p)-1]
        return nf
    return f


def get_libdevice_functions():
    """Return all libdevice function names with prefix `__nv_` removed.

    Reference: https://docs.nvidia.com/cuda/libdevice-users-guide/function-desc.html#function-desc
    """
    return list('''
abs acos acosf acosh acoshf asin asinf asinh asinhf atan atan2 atan2f
atanf atanh atanhf brev brevll byte_perm cbrt cbrtf ceil ceilf clz
clzll copysign copysignf cos cosf cosh coshf cospi cospif dadd_rd
dadd_rn dadd_ru dadd_rz ddiv_rd ddiv_rn ddiv_ru ddiv_rz dmul_rd
dmul_rn dmul_ru dmul_rz double2float_rd double2float_rn
double2float_ru double2float_rz double2hiint double2int_rd
double2int_rn double2int_ru double2int_rz double2ll_rd double2ll_rn
double2ll_ru double2ll_rz double2loint double2uint_rd double2uint_rn
double2uint_ru double2uint_rz double2ull_rd double2ull_rn
double2ull_ru double2ull_rz double_as_longlong drcp_rd drcp_rn drcp_ru
drcp_rz dsqrt_rd dsqrt_rn dsqrt_ru dsqrt_rz erf erfc erfcf erfcinv
erfcinvf erfcx erfcxf erff erfinv erfinvf exp exp10 exp10f exp2 exp2f
expf expm1 expm1f fabs fabsf fadd_rd fadd_rn fadd_ru fadd_rz fast_cosf
fast_exp10f fast_expf fast_fdividef fast_log10f fast_log2f fast_logf
fast_powf fast_sincosf fast_sinf fast_tanf fdim fdimf fdiv_rd fdiv_rn
fdiv_ru fdiv_rz ffs ffsll finitef float2half_rn float2int_rd
float2int_rn float2int_ru float2int_rz float2ll_rd float2ll_rn
float2ll_ru float2ll_rz float2uint_rd float2uint_rn float2uint_ru
float2uint_rz float2ull_rd float2ull_rn float2ull_ru float2ull_rz
float_as_int floor floorf fma fma_rd fma_rn fma_ru fma_rz fmaf fmaf_rd
fmaf_rn fmaf_ru fmaf_rz fmax fmaxf fmin fminf fmod fmodf fmul_rd
fmul_rn fmul_ru fmul_rz frcp_rd frcp_rn frcp_ru frcp_rz frexp frexpf
frsqrt_rn fsqrt_rd fsqrt_rn fsqrt_ru fsqrt_rz fsub_rd fsub_rn fsub_ru
fsub_rz hadd half2float hiloint2double hypot hypotf ilogb ilogbf
int2double_rn int2float_rd int2float_rn int2float_ru int2float_rz
int_as_float isfinited isinfd isinff isnand isnanf j0 j0f j1 j1f jn
jnf ldexp ldexpf lgamma lgammaf ll2double_rd ll2double_rn ll2double_ru
ll2double_rz ll2float_rd ll2float_rn ll2float_ru ll2float_rz llabs
llmax llmin llrint llrintf llround llroundf log log10 log10f log1p
log1pf log2 log2f logb logbf logf longlong_as_double max min modf
modff mul24 mul64hi mulhi nan nanf nearbyint nearbyintf nextafter
nextafterf normcdf normcdff normcdfinv normcdfinvf popc popcll pow
powf powi powif rcbrt rcbrtf remainder remainderf remquo remquof rhadd
rint rintf round roundf rsqrt rsqrtf sad saturatef scalbn scalbnf
signbitd signbitf sin sincos sincosf sincospi sincospif sinf sinh
sinhf sinpi sinpif sqrt sqrtf tan tanf tanh tanhf tgamma tgammaf trunc
truncf uhadd uint2double_rn uint2float_rd uint2float_rn uint2float_ru
uint2float_rz ull2double_rd ull2double_rn ull2double_ru ull2double_rz
ull2float_rd ull2float_rn ull2float_ru ull2float_rz ullmax ullmin umax
umin umul24 umul64hi umulhi urhadd usad y0 y0f y1 y1f yn ynf
    '''.strip().split())


def get_nvvm_intrinsic_functions():
    """Return all nvvm intrinsic function names with prefix `llvm.` removed.

    Reference: https://docs.nvidia.com/cuda/nvvm-ir-spec/index.html#intrinsic-functions
    """
    return list('''
memcpy memmove memset sqrt fma bswap ctpop ctlz cttz fmuladd
convert.to.fp16.f32 convert.from.fp16.f32 convert.to.fp16
convert.from.fp16 lifetime.start lifetime.end invariant.start
invariant.end var.annotation ptr.annotation annotation expect
donothing
'''.strip().split())


def get_llvm_intrinsic_functions():
    """Return all llvm intrinsic function names with prefix `llvm.` removed.

    Reference: https://llvm.org/docs/LangRef.html#intrinsic-functions
    """
    return list('''
va_start va_end va_copy gcroot gcread gcwrite returnaddress
addressofreturnaddress sponentry frameaddress stacksave stackrestore
get.dynamic.area.offset prefetch pcmarker readcyclecounter clear_cache
instrprof.increment instrprof.increment.step instrprof.value.profile
thread.pointer call.preallocated.setup call.preallocated.arg
call.preallocated.teardown abs smax smin umax umin memcpy
memcpy.inline memmove sqrt powi sin cos pow exp exp2
log log10 log2 fma fabs minnum maxnum minimum
maximum copysign floor ceil trunc rint nearbyint round
roundeven lround llround lrint llrint ctpop ctlz cttz
fshl fshr canonicalize fmuladd set.loop.iterations
test.set.loop.iterations loop.decrement.reg loop.decrement
vector.reduce.add vector.reduce.fadd vector.reduce.mul
vector.reduce.fmul vector.reduce.and vector.reduce.or
vector.reduce.xor vector.reduce.smax vector.reduce.smin
vector.reduce.umax vector.reduce.umin vector.reduce.fmax
vector.reduce.fmin matrix.transpose matrix.multiply
matrix.column.major.load matrix.column.major.store convert.to.fp16
convert.from.fp16 init.trampoline adjust.trampoline lifetime.start
lifetime.end invariant.start invariant.end launder.invariant.group
strip.invariant.group experimental.constrained.fadd
experimental.constrained.fsub experimental.constrained.fmul
experimental.constrained.fdiv experimental.constrained.frem
experimental.constrained.fma experimental.constrained.fptoui
experimental.constrained.fptosi experimental.constrained.uitofp
experimental.constrained.sitofp experimental.constrained.fptrunc
experimental.constrained.fpext experimental.constrained.fmuladd
experimental.constrained.sqrt experimental.constrained.pow
experimental.constrained.powi experimental.constrained.sin
experimental.constrained.cos experimental.constrained.exp
experimental.constrained.exp2 experimental.constrained.log
experimental.constrained.log10 experimental.constrained.log2
experimental.constrained.rint experimental.constrained.lrint
experimental.constrained.llrint experimental.constrained.nearbyint
experimental.constrained.maxnum experimental.constrained.minnum
experimental.constrained.maximum experimental.constrained.minimum
experimental.constrained.ceil experimental.constrained.floor
experimental.constrained.round experimental.constrained.roundeven
experimental.constrained.lround experimental.constrained.llround
experimental.constrained.trunc flt.rounds var.annotation
ptr.annotation annotation codeview.annotation trap debugtrap
stackprotector stackguard objectsize expect expect.with.probability
assume ssa_copy type.test type.checked.load donothing
experimental.deoptimize experimental.guard
experimental.widenable.condition load.relative sideeffect
is.constant ptrmask vscale memcpy.element.unordered.atomic
memmove.element.unordered.atomic memset.element.unordered.atomic
objc.autorelease objc.autoreleasePoolPop objc.autoreleasePoolPush
objc.autoreleaseReturnValue objc.copyWeak objc.destroyWeak
objc.initWeak objc.loadWeak objc.loadWeakRetained objc.moveWeak
objc.release objc.retain objc.retainAutorelease
objc.retainAutoreleaseReturnValue objc.retainAutoreleasedReturnValue
objc.retainBlock objc.storeStrong objc.storeWeak
preserve.array.access.index preserve.union.access.index
preserve.struct.access.index
'''.strip().split())


def get_libm_functions():
    """Return all GNU m library function names.

    Reference: https://www.gnu.org/software/libc/manual/html_node/Mathematics.html
    """
    return list('''
sin sinf sinl cos cosf cosl tan tanf tanl sincos sincosf sincosl
csin csinf csinl ccos ccosf ccosl ctan ctanf ctanl asin asinf asinl
acos acosf acosl atan atanf atanl atan2 atan2f atan2l casin casinf
casinl cacos cacosf cacosl catan catanf catanl exp expf expl exp2
exp2f exp2l exp10 exp10f exp10l log logf logl log2 log2f log2l log10
log10f log10l logb logbf logbl ilogb ilogbf ilogbl pow powf powl sqrt
sqrtf sqrtl cbrt cbrtf cbrtl hypot hypotf hypotl expm1 expm1f expm1l
log1p log1pf log1pl clog clogf clogl clog10 clog10f clog10l csqrt
csqrtf csqrtl cpow cpowf cpowl sinh sinhf sinhl cosh coshf coshl tanh
tanhf tanhl csinh csinhf csinhl ccosh ccoshf ccoshl ctanh ctanhf
ctanhl asinh asinhf asinhl acosh acoshf acoshl atanh atanhf atanhl
casinh casinhf casinhl cacosh cacoshf cacoshl catanh catanhf catanhl
erf erff erfl erfc erfcf erfcl lgamma lgammaf lgammal lgamma_r
lgammaf_r lgammal_r gamma gammaf gammal j0 j0f j0l j1 j1f j1l jn jnf
jnl y0 y0f y0l y1 y1f y1l yn ynf ynl rand srand rand_r random srandom
initstate setstate random_r srandom_r initstate_r setstate_r drand48
erand48 lrand48 nrand48 mrand48 jrand48 srand48 seed48 lcong48
drand48_r erand48_r lrand48_r nrand48_r mrand48_r jrand48_r srand48_r
seed48_r lcong48_r

abs labs llabs fabs fabsf fabsl cabs cabsf cabsl frexp frexpf frexpl
ldexp ldexpf ldexpl scalb scalbf scalbl scalbn scalbnf scalbnl
significand significandf significandl ceil ceilf ceill floor floorf
floorl trunc truncf truncl rint rintf rintl nearbyint nearbyintf
nearbyintl round roundf roundl roundeven roundevenf roundevenl lrint
lrintf lrintl lround lroundf lroundl llround llroundf llroundl fromfp
fromfpf fromfpl ufromfp ufromfpf ufromfpl fromfpx fromfpxf fromfpxl
ufromfpx ufromfpxf ufromfpxl modf modff modfl fmod fmodf fmodl
remainder remainderf remainderl drem dremf dreml

copysign copysignf copysignl signbit signbitf signbitl nextafter
nextafterf nextafterl nexttoward nexttowardf nexttowardl nextup
nextupf nextupl nextdown nextdownf nextdownl nan nanf nanl
canonicalize canonicalizef canonicalizel getpayload getpayloadf
getpayloadl setpayload setpayloadf setpayloadl setpayloadsig
setpayloadsigf setpayloadsigl isgreater isgreaterequal isless
islessequal islessgreater isunordered iseqsig totalorder totalorderf
totalorderl totalordermag totalorderf totalorderl fmin fminf fminl
fmax fmaxf fmaxl fminmag fminmagf fminmagl fmaxmag fmaxmagf fmaxmagl
fdim fdimf fdiml fma fmaf fmal fadd faddf faddl fsub fsubf fsubl fmul
fmulf fmull fdiv fdivf fdivl '''.strip().split())


def get_stdio_functions():
    """Return all stdio library function names.

    Reference: http://www.cplusplus.com/reference/cstdio/
    """
    return list(''' remove rename tmpfile tmpnam fclose fflush fopen freopen setbuf
setvbuf fprintf fscanf printf scanf snprintf sprintf sscanf vfprintf
vfscanf vprintf vscanf vsnprintf vsprintf vsscanf fgetc fgets fputc
fputs getc getchar gets putc putchar puts ungetc fread fwrite fgetpos
fseek fsetpos ftell rewind clearerr feof ferror perror '''.strip().split())


def get_stdlib_functions():
    """Return all stdlib library function names.

    Reference: http://www.cplusplus.com/reference/cstdlib/
    """
    return list('''
atof atoi atol atoll strtod strtof strtol strtold strtoll strtoul
strtoull rand srand calloc free malloc realloc abort atexit
at_quick_exit exit getenv quick_exit system bsearch qsort abs div labs
ldiv llabs lldiv mblen mbtowc wctomb mbstowcs wcstombs
'''.strip().split())
