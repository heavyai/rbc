# Author: Pearu Peterson
# Created: February 2019

import re
import warnings
from collections import defaultdict, deque
from contextlib import contextmanager
from typing import Optional, Dict, Set

import llvmlite.binding as llvm
from llvmlite import ir
from numba.core import cgutils, codegen, compiler, compiler_lock, cpu
from numba.core import errors as nb_errors
from numba.core import extending, imputils, registry, sigutils, typing

from rbc.externals import stdio
from rbc.nrt import create_nrt_functions, read_unicodetype_db
from rbc import config

from .errors import UnsupportedError
from .libfuncs import Library
from .targetinfo import TargetInfo

int32_t = ir.IntType(32)
int1_t = ir.IntType(1)


def nrt_required(main_module: llvm.module.ModuleRef):
    for f in main_module.functions:
        if f.is_declaration and f.name.startswith('NRT'):
            return True
    return False


def find_at_word(text: str) -> Optional[str]:
    """
    Find called function name from a CallInst to a bitcast.

    Example usage

        >>> text = "i8* (i64)* bitcast (%Struct.MemInfo.5* (i64)* @NRT_MemInfo_alloc_safe to i8* (i64)*)"  # noqa: E501
        >>> find_at_word(text)
        "NRT_MemInfo_alloc_safe
        >>> text = "%.5 = bitcast i8* %info to void (i8*, i64, i8*)*"
        >>> find_at_word(text)
        None

    """
    pattern = r"@\w+"
    words = re.findall(pattern, text)
    if words == []:
        return None
    assert len(words) == 1
    [word] = words
    return word[1:]


def get_called_functions(library,
                         funcname: Optional[str] = None,
                         debug: bool = False) -> Dict[str, Set[str]]:
    result = defaultdict(set)
    q: deque[str] = deque()

    module = library._final_module
    if funcname is None:
        for df in library.get_defined_functions():
            q.append(df.name)
    else:
        q.append(funcname)

    linked_functions: Dict[str, JITRemoteCodeLibrary] = dict()
    for lib in library._linking_libraries:
        result['libraries'].add(lib)
        for df in lib.get_defined_functions():
            linked_functions[df.name] = lib

    while len(q) > 0:
        funcname = q.pop()

        if funcname in result['defined']:
            continue

        func = module.get_function(funcname)
        assert func.name == funcname, (func.name, funcname)
        result['defined'].add(funcname)

        for block in func.blocks:
            for instruction in block.instructions:
                if instruction.opcode == 'call':
                    instr = list(instruction.operands)[-1]
                    name = instr.name
                    try:
                        f = module.get_function(name)
                    except NameError:
                        if 'bitcast' not in str(instr):
                            raise  # re-raise

                        # attempt to find caller symbol in instr
                        name = find_at_word(str(instr))
                        if name is None:
                            # ignore call to function pointer
                            msg = f'Ignoring call to bitcast instruction:\n{instr}'
                            if debug:
                                warnings.warn(msg)
                            continue
                        f = module.get_function(name)

                    if name.startswith('llvm.'):
                        result['intrinsics'].add(name)
                    elif f.is_declaration:
                        if name in linked_functions:
                            lib = linked_functions.get(name)
                            result['defined'].add(name)
                            result['libraries'].add(lib)
                            q.append(name)
                            break
                        else:
                            result['declarations'].add(name)
                    else:
                        q.append(name)
    return result


# ---------------------------------------------------------------------------

class JITRemoteCodeLibrary(codegen.JITCodeLibrary):
    """JITRemoteCodeLibrary was introduce to prevent numba from calling functions
    that checks if the module is final. See xnd-project/rbc issue #87.
    """

    def get_pointer_to_function(self, name):
        """We can return any random number here! This is just to prevent numba from
        trying to check if the symbol given by "name" is defined in the module.
        In cases were RBC is calling an external function (i.e. allocate_varlen_buffer)
        the symbol will not be defined in the module, resulting in an error.
        """
        return 0

    def _finalize_specific(self):
        """Same as codegen.JITCodeLibrary._finalize_specific but without
        calling _ensure_finalize at the end
        """
        self._codegen._scan_and_fix_unresolved_refs(self._final_module)


class JITRemoteCodegen(codegen.JITCPUCodegen):
    _library_class = JITRemoteCodeLibrary

    def _get_host_cpu_name(self):
        target_info = TargetInfo()
        return target_info.device_name

    def _get_host_cpu_features(self):
        target_info = TargetInfo()
        features = target_info.device_features
        server_llvm_version = target_info.llvm_version
        if server_llvm_version is None or target_info.is_gpu:
            return ''
        client_llvm_version = llvm.llvm_version_info

        # See https://github.com/xnd-project/rbc/issues/45
        remove_features = {
            (12, 12): [], (11, 11): [], (10, 10): [], (9, 9): [], (8, 8): [],
            (11, 8): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'tsxldtrk', 'amx-tile', 'amx-bf16',
                      'serialize', 'amx-int8', 'avx512vp2intersect', 'tsxldtrk',
                      'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'cx8', 'enqcmd', 'avx512bf16'],
            (11, 10): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8'],
            (9, 11): ['sse2', 'cx16', 'sahf', 'tbm', 'avx512ifma', 'sha',
                      'gfni', 'fma4', 'vpclmulqdq', 'prfchw', 'bmi2', 'cldemote',
                      'fsgsbase', 'ptwrite', 'xsavec', 'popcnt', 'mpx',
                      'avx512bitalg', 'movdiri', 'xsaves', 'avx512er',
                      'avx512vnni', 'avx512vpopcntdq', 'pconfig', 'clwb',
                      'avx512f', 'clzero', 'pku', 'mmx', 'lwp', 'rdpid', 'xop',
                      'rdseed', 'waitpkg', 'movdir64b', 'sse4a', 'avx512bw',
                      'clflushopt', 'xsave', 'avx512vbmi2', '64bit', 'avx512vl',
                      'invpcid', 'avx512cd', 'avx', 'vaes', 'cx8', 'fma', 'rtm',
                      'bmi', 'enqcmd', 'rdrnd', 'mwaitx', 'sse4.1', 'sse4.2', 'avx2',
                      'fxsr', 'wbnoinvd', 'sse', 'lzcnt', 'pclmul', 'prefetchwt1',
                      'f16c', 'ssse3', 'sgx', 'shstk', 'cmov', 'avx512vbmi',
                      'avx512bf16', 'movbe', 'xsaveopt', 'avx512dq', 'adx',
                      'avx512pf', 'sse3'],
            (9, 8): ['cx8', 'enqcmd', 'avx512bf16'],
            (14, 11): ['crc32', 'uintr', 'widekl', 'avxvnni', 'avx512fp16', 'kl', 'hreset'],
        }.get((server_llvm_version[0], client_llvm_version[0]), None)
        if remove_features is None:
            warnings.warn(
                f'{type(self).__name__}._get_host_cpu_features: `remove_features` dictionary'
                ' requires an update: detected different LLVM versions in server '
                f'{server_llvm_version} and client {client_llvm_version}.'
                f' CPU features: {features}.')
        else:
            features += ','
            for f in remove_features:
                features = features.replace('+' + f + ',', '').replace('-' + f + ',', '')
            features.rstrip(',')
        return features

    def _customize_tm_options(self, options):
        super()._customize_tm_options(options)
        # fix reloc_model as the base method sets it using local target
        target_info = TargetInfo()
        if target_info.arch.startswith('x86'):
            reloc_model = 'static'
        else:
            reloc_model = 'default'
        options['reloc'] = reloc_model

    def set_env(self, env_name, env):
        return None


class JITRemoteTypingContext(typing.Context):
    def load_additional_registries(self):
        self.install_registry(typing.templates.builtin_registry)
        super().load_additional_registries()


class JITRemoteTargetContext(cpu.CPUContext):

    @compiler_lock.global_compiler_lock
    def init(self):
        target_info = TargetInfo()
        self.address_size = target_info.bits
        self.is32bit = (self.address_size == 32)
        self._internal_codegen = JITRemoteCodegen("numba.exec")

    def load_additional_registries(self):
        self.install_registry(imputils.builtin_registry)
        super().load_additional_registries()

    def get_executable(self, library, fndesc, env):
        return None

    def post_lowering(self, mod, library):
        pass


# ---------------------------------------------------------------------------
# Code generation methods


@contextmanager
def replace_numba_internals_hack():
    # Hackish solution to prevent numba from calling _ensure_finalize. See issue #87
    _internal_codegen_bkp = registry.cpu_target.target_context._internal_codegen
    registry.cpu_target.target_context._internal_codegen = JITRemoteCodegen("numba.exec")
    yield
    registry.cpu_target.target_context._internal_codegen = _internal_codegen_bkp


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
        wrapfn = ir.Function(module, wrapty, fname)
        builder = ir.IRBuilder(wrapfn.append_basic_block('entry'))
        fnty = context.call_conv.get_function_type(rtype, atypes)
        fn = ir.Function(builder.module, fnty, cres.fndesc.llvm_func_name)
        status, out = context.call_conv.call_function(
            builder, fn, rtype, atypes, wrapfn.args[1:])
        with cgutils.if_unlikely(builder, status.is_error):
            if verbose and target.is_cpu:
                cgutils.printf(builder,
                               f"rbc: {fname} failed with status code %i\n",
                               status.code)
                stdio._cg_fflush(builder)
            builder.ret_void()
        # does a "deepcopy"
        rtype.deepcopy(context, builder, out, wrapfn.args[0])
        builder.ret_void()
    else:
        wrapty = ir.FunctionType(ll_return_type, ll_argtypes)
        wrapfn = ir.Function(module, wrapty, fname)
        builder = ir.IRBuilder(wrapfn.append_basic_block('entry'))
        fnty = context.call_conv.get_function_type(rtype, atypes)
        fn = ir.Function(builder.module, fnty, cres.fndesc.llvm_func_name)
        status, out = context.call_conv.call_function(
            builder, fn, rtype, atypes, wrapfn.args)
        if verbose and target.is_cpu:
            with cgutils.if_unlikely(builder, status.is_error):
                cgutils.printf(builder,
                               f"rbc: {fname} failed with status code %i\n",
                               status.code)
                stdio._cg_fflush(builder)
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
    flags.no_compile = True
    flags.no_cpython_wrapper = True
    flags.no_cfunc_wrapper = True
    flags.nrt = True

    fname = func.__name__ + sig.mangling()
    args, return_type = sigutils.normalize_signature(
        sig.tonumba(bool_is_int8=True))
    target.set_compile_target(fname)
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
        target.set_compile_target(None)
    except UnsupportedError as msg:
        target.set_compile_target(None)
        warnings.warn(f'Skipping {fname}: {msg.args[1]}')
        return
    except (nb_errors.TypingError, nb_errors.LoweringError) as msg:
        target.set_compile_target(None)
        for m in re.finditer(r'UnsupportedError(.*?)\n', str(msg), re.S):
            warnings.warn(f'Skipping {fname}:{m.group(0)[18:]}')
            break
        else:
            raise
        return
    except Exception:
        target.set_compile_target(None)
        raise

    result = get_called_functions(cres.library, cres.fndesc.llvm_func_name, debug)

    for f in result['declarations']:
        if target.supports(f):
            continue
        warnings.warn(f'Skipping {fname} that uses undefined function `{f}`')
        return

    nvvmlib = Library.get('nvvm')
    llvmlib = Library.get('llvm')
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


def add_metadata_flag(main_library, **kwargs):
    module = ir.Module()
    mflags = module.add_named_metadata('llvm.module.flags')
    override_flag = int32_t(4)
    for flag_name, flag_value in kwargs.items():
        flag = module.add_metadata([override_flag, flag_name, int1_t(flag_value)])
        mflags.add(flag)
    main_library.add_ir_module(module)


def compile_to_LLVM(functions_and_signatures,
                    target_info: TargetInfo,
                    pipeline_class=compiler.Compiler,
                    user_defined_llvm_ir=None,
                    debug=False):
    """Compile functions with given signatures to target specific LLVM IR.

    Parameters
    ----------
    functions_and_signatures : list
      Specify a list of Python function and its signatures pairs.
    target : TargetInfo
      Specify target device information.
    user_defined_llvm_ir : {None, str, ModuleRef}
      Specify user-defined LLVM IR module that is linked in to the
      returned module.
    debug : bool

    Returns
    -------
    module : llvmlite.binding.ModuleRef
      LLVM module instance. To get the IR string, use `str(module)`.

    """
    target_desc = registry.cpu_target

    typing_context = JITRemoteTypingContext()
    target_context = JITRemoteTargetContext(typing_context)

    if config.ENABLE_NRT:
        # both the nrt_module and unicodetype_db are cached
        nrt_module = create_nrt_functions(target_context, debug=debug)
        unicodetype_db = read_unicodetype_db()

    # Bring over Array overloads (a hack):
    target_context._defns = target_desc.target_context._defns

    with replace_numba_internals_hack():
        codegen = target_context.codegen()
        main_library = codegen.create_library('rbc.irtools.compile_to_IR')
        main_module = main_library._final_module

        if user_defined_llvm_ir is not None:
            if isinstance(user_defined_llvm_ir, str):
                user_defined_llvm_ir = llvm.parse_assembly(user_defined_llvm_ir)
            assert isinstance(user_defined_llvm_ir, llvm.ModuleRef)
            main_module.link_in(user_defined_llvm_ir, preserve=True)

        succesful_fids = []
        function_names = []
        for func, signatures in functions_and_signatures:
            for fid, sig in signatures.items():
                fname = compile_instance(func, sig, target_info, typing_context,
                                         target_context, pipeline_class,
                                         main_library,
                                         debug=debug)
                if fname is not None:
                    succesful_fids.append(fid)
                    function_names.append(fname)

        if nrt_required(main_module):
            if config.ENABLE_NRT:
                main_module.link_in(unicodetype_db)
                main_library.add_ir_module(nrt_module)
            else:
                msg = ("NRT is disabled but required. Undefine the environment "
                       "variable 'RBC_ENABLE_NRT' or set its value to '1'")
                warnings.warn(msg)

        add_metadata_flag(main_library,
                          pass_column_arguments_by_value=0,
                          manage_memory_buffer=1)
        main_library._optimize_final_module()

        # Remove unused defined functions and declarations
        used_symbols = defaultdict(set)
        for fname in function_names:
            symbols = get_called_functions(main_library, fname, debug)
            for k, v in symbols.items():
                used_symbols[k].update(v)

        all_symbols = get_called_functions(main_library, debug=debug)

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
        main_module.triple = target_info.triple
        main_module.data_layout = target_info.datalayout

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
    llvm.initialize()
    llvm.initialize_all_targets()
    llvm.initialize_all_asmprinters()

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


def IS_CPU():
    pass


@extending.overload(IS_CPU, inline="always")
def is_cpu_impl():
    target_info = TargetInfo()
    if target_info.is_cpu:
        return lambda: True
    else:
        return lambda: False


def IS_GPU():
    pass


@extending.overload(IS_GPU, inline="always")
def is_gpu_impl():
    target_info = TargetInfo()
    if target_info.is_gpu:
        return lambda: True
    else:
        return lambda: False
