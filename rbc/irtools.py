# Author: Pearu Peterson
# Created: February 2019

import re
import warnings
from collections import defaultdict
from llvmlite import ir
import llvmlite.binding as llvm
from .targetinfo import TargetInfo
from .utils import get_version
from .errors import UnsupportedError
from . import libfuncs

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


def get_called_functions(library, funcname=None):
    result = defaultdict(set)
    module = library._final_module
    if funcname is None:
        for df in library.get_defined_functions():
            for k, v in get_called_functions(library, df.name).items():
                result[k].update(v)
        return result

    func = module.get_function(funcname)
    assert func.name == funcname, (func.name, funcname)
    result['defined'].add(funcname)
    for block in func.blocks:
        for instruction in block.instructions:
            if instruction.opcode == 'call':
                name = list(instruction.operands)[-1].name
                f = module.get_function(name)
                if name.startswith('llvm.'):
                    result['intrinsics'].add(name)
                elif f.is_declaration:
                    found = False
                    for lib in library._linking_libraries:
                        for df in lib.get_defined_functions():
                            if name == df.name:
                                result['defined'].add(name)
                                result['libraries'].add(lib)
                                found = True
                                for k, v in get_called_functions(lib, name).items():
                                    result[k].update(v)
                                break
                        if found:
                            break
                    if not found:
                        result['declarations'].add(name)
                else:
                    result['defined'].add(name)
                    for k, v in get_called_functions(library, name).items():
                        result[k].update(v)
    return result


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
        server_llvm_version = self.target_info.llvm_version
        if server_llvm_version is None:
            return ''
        client_llvm_version = llvm.llvm_version_info

        # See https://github.com/xnd-project/rbc/issues/45
        remove_features = {
            (11, 8): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'tsxldtrk', 'amx-tile', 'amx-bf16',
                      'serialize', 'amx-int8', 'avx512vp2intersect', 'tsxldtrk',
                      'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'cx8', 'enqcmd', 'avx512bf16'],
            (9, 8): ['cx8', 'enqcmd', 'avx512bf16'],
        }.get((server_llvm_version[0], client_llvm_version[0]), [])
        for f in remove_features:
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


def make_wrapper(fname, atypes, rtype, cres, target: TargetInfo, verbose=False):
    """Make wrapper function to numba compile result.

    The compilation result contains a function with prototype::

      <status> <function name>(<rtype>** result, <arguments>)

    The make_wrapper adds a wrapper function to compilation result
    library with the following prototype::

      <rtype> <fname>(<arguments>)

    or, if <rtype>.return_as_first_argument == True, then

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
      When True, insert printf statements for debugging errors. For
      CUDA target this feature is disabled.

    """
    fndesc = cres.fndesc
    module = cres.library.create_ir_module(fndesc.unique_name)
    context = cres.target_context
    ll_argtypes = [context.get_value_type(ty) for ty in atypes]
    ll_return_type = context.get_value_type(rtype)

    return_as_first_argument = getattr(rtype, 'return_as_first_argument', False)
    assert isinstance(return_as_first_argument, bool)
    if return_as_first_argument:
        wrapty = ir.FunctionType(ir.VoidType(),
                                 [ll_return_type] + ll_argtypes)
        wrapfn = module.add_function(wrapty, fname)
        builder = ir.IRBuilder(wrapfn.append_basic_block('entry'))
        fnty = context.call_conv.get_function_type(rtype, atypes)
        fn = builder.module.add_function(fnty, cres.fndesc.llvm_func_name)
        status, out = context.call_conv.call_function(
            builder, fn, rtype, atypes, wrapfn.args[1:])
        with cgutils.if_unlikely(builder, status.is_error):
            if verbose and target.is_cpu:
                cgutils.printf(builder,
                               f"rbc: {fname} failed with status code %i\n",
                               status.code)
                cg_fflush(builder)
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
        if verbose and target.is_cpu:
            with cgutils.if_unlikely(builder, status.is_error):
                cgutils.printf(builder,
                               f"rbc: {fname} failed with status code %i\n",
                               status.code)
                cg_fflush(builder)
        builder.ret(out)

    cres.library.add_ir_module(module)


def compile_instance(func, sig,
                     target: TargetInfo,
                     typing_context,
                     target_context,
                     pipeline_class,
                     main_library,
                     debug=False):
    """Compile a function with given signature. Return function name when
    succesful.
    """
    flags = compiler.Flags()
    flags.set('no_compile')
    flags.set('no_cpython_wrapper')
    if get_version('numba') >= (0, 49):
        flags.set('no_cfunc_wrapper')

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
    except UnsupportedError as msg:
        for m in re.finditer(r'[|]UnsupportedError[|](.*?)\n', str(msg), re.S):
            warnings.warn(f'Skipping {fname}: {m.group(0)[18:]}')
        return
    except nb_errors.TypingError as msg:
        for m in re.finditer(r'[|]UnsupportedError[|](.*?)\n', str(msg), re.S):
            warnings.warn(f'Skipping {fname}: {m.group(0)[18:]}')
            break
        else:
            raise
        return
    except Exception:
        raise

    result = get_called_functions(cres.library, cres.fndesc.llvm_func_name)

    for f in result['declarations']:
        if target.supports(f):
            continue
        warnings.warn(f'Skipping {fname} that uses undefined function `{f}`')
        return

    nvvmlib = libfuncs.Library.get('nvvm')
    llvmlib = libfuncs.Library.get('llvm')
    for f in result['intrinsics']:
        if target.is_gpu:
            if f in nvvmlib:
                continue

        if target.is_cpu:
            if f in llvmlib:
                continue

        warnings.warn(f'Skipping {fname} that uses unsupported intrinsic `{f}`')
        return

    make_wrapper(fname, args, return_type, cres, target, verbose=debug)

    main_module = main_library._final_module
    for lib in result['libraries']:
        main_module.link_in(
            lib._get_module_for_linking(), preserve=True,
        )

    return fname


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
        elif target.is_gpu:
            typing_context = RemoteGPUTypingContext()
            target_context = RemoteGPUTargetContext(typing_context, target)
        else:
            raise ValueError(f'Unknown target {target.name}')

        # Bring over Array overloads (a hack):
        target_context._defns = target_desc.target_context._defns

    codegen = target_context.codegen()
    main_library = codegen.create_library('rbc.irtools.compile_to_IR')
    main_module = main_library._final_module

    succesful_fids = []
    function_names = []
    for func, signatures in functions_and_signatures:
        for fid, sig in signatures.items():
            fname = compile_instance(func, sig, target, typing_context,
                                     target_context, pipeline_class,
                                     main_library,
                                     debug=debug)
            if fname is not None:
                succesful_fids.append(fid)
                function_names.append(fname)

    main_library._optimize_final_module()

    # Remove unused defined functions and declarations
    used_symbols = defaultdict(set)
    for fname in function_names:
        for k, v in get_called_functions(main_library, fname).items():
            used_symbols[k].update(v)

    all_symbols = get_called_functions(main_library)

    unused_symbols = defaultdict(set)
    for k, lst in all_symbols.items():
        if k == 'libraries':
            continue
        for fn in lst:
            if fn not in used_symbols[k]:
                unused_symbols[k].add(fn)

    changed = False
    for f in main_module.functions:
        fn = f.name
        if fn.startswith('llvm.'):
            if f.name in unused_symbols['intrinsics']:
                f.linkage = llvm.Linkage.external
                changed = True
        elif f.is_declaration:
            if f.name in unused_symbols['declarations']:
                f.linkage = llvm.Linkage.external
                changed = True
        else:
            if f.name in unused_symbols['defined']:
                f.linkage = llvm.Linkage.private
                changed = True

    # TODO: determine unused global_variables and struct_types

    if changed:
        main_library._optimize_final_module()

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
        target_info = TargetInfo()
        if target_info.is_cpu:
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
            target_info = TargetInfo()
            if target_info.is_cpu:
                cgutils.printf(builder, format_type.literal_value, *args[1:])
                cg_fflush(builder)

        return sig, codegen

    else:
        raise TypeError(f'expected StringLiteral but got {type(format_type).__name__}')
