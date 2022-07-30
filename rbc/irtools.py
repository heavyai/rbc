# Author: Pearu Peterson
# Created: February 2019

import re
import warnings
from collections import defaultdict
from llvmlite import ir
import llvmlite.binding as llvm
from .targetinfo import TargetInfo
from .errors import UnsupportedError
from . import libfuncs
from rbc.externals import stdio
from numba.core import cpu, \
    registry, typing, compiler, sigutils, cgutils, \
    extending, target_extension, retarget, dispatcher
from numba import njit
from numba.core import errors as nb_errors


class Retarget(retarget.BasicRetarget):

    def __init__(self, target_name):
        self.target_name = target_name
        super().__init__()

    @property
    def output_target(self):
        return self.target_name

    def compile_retarget(self, cpu_disp):
        kernel = njit(_target=self.target_name)(cpu_disp.py_func)
        return kernel


def switch_target(target_name):
    tc = dispatcher.TargetConfigurationStack
    return tc.switch_target(Retarget(target_name))


int32_t = ir.IntType(32)
int1_t = ir.IntType(1)


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
                     target_info: TargetInfo,
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
    except (UnsupportedError, nb_errors.TypingError, nb_errors.LoweringError) as msg:
        for m in re.finditer(r'UnsupportedError(.*?)\n', str(msg), re.S):
            warnings.warn(f'Skipping {fname}:{m.group(0)[18:]}')
            break
        else:
            raise
        return
    except Exception:
        raise

    result = get_called_functions(cres.library, cres.fndesc.llvm_func_name)

    for f in result['declarations']:
        if target_info.supports(f):
            continue
        warnings.warn(f'Skipping {fname} that uses undefined function `{f}`')
        return

    nvvmlib = libfuncs.Library.get('nvvm')
    llvmlib = libfuncs.Library.get('llvm')
    for f in result['intrinsics']:
        if target_info.is_gpu:
            if f in nvvmlib:
                continue

        if target_info.is_cpu:
            if f in llvmlib:
                continue

        warnings.warn(f'Skipping {fname} that uses unsupported intrinsic `{f}`')
        return

    make_wrapper(fname, args, return_type, cres, target_info, verbose=debug)

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
    # avoid circula import error
    # * remotejit imports irtools
    # * irtools import heavydb
    # * heavydb import remotejit
    from rbc.heavydb import JITRemoteTypingContext, JITRemoteTargetContext, \
        heavydb_cpu_target, heavydb_gpu_target

    device = target_info.name
    software = target_info.software[0]

    if software == 'HeavyDB':
        target_name = f'heavydb_{device}'
        target_desc = heavydb_cpu_target if device == 'cpu' else heavydb_gpu_target
        typing_context = JITRemoteTypingContext()
        target_context = JITRemoteTargetContext(typing_context, target_name)
    else:
        target_name = 'cpu'
        target_desc = registry.cpu_target
        typing_context = typing.Context()
        target_context = cpu.CPUContext(typing_context, target_name)

    # Bring over Array overloads (a hack):
    target_context._defns = target_desc.target_context._defns

    codegen = target_context.codegen()
    main_library = codegen.create_library(f'rbc.irtools.compile_to_IR_{software}_{device}')
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
            with switch_target(target_name):
                with target_extension.target_override(target_name):
                    fname = compile_instance(func, sig, target_info, typing_context,
                                             target_context, pipeline_class,
                                             main_library,
                                             debug=debug)
                    if fname is not None:
                        succesful_fids.append(fid)
                        function_names.append(fname)

    add_metadata_flag(main_library,
                      pass_column_arguments_by_value=0,
                      manage_memory_buffer=1)
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
