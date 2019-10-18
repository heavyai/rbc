# Author: Pearu Peterson
# Created: February 2019

import re
import numba as nb
from llvmlite import ir
import llvmlite.binding as llvm
from .utils import is_localhost, triple_matches


def initialize_llvm():
    llvm.initialize()
    llvm.initialize_all_targets()
    llvm.initialize_all_asmprinters()


def get_function_dependencies(module, funcname, _deps=None):
    if _deps is None:
        _deps = dict()
    func = module.get_function(funcname)
    assert func.name == funcname
    for block in func.blocks:
        for instruction in block.instructions:
            if instruction.opcode == 'call':
                name = list(instruction.operands)[-1].name
                f = module.get_function(name)
                if name.startswith('llvm.'):
                    _deps[name] = 'intrinsic'
                elif f.is_declaration:
                    _deps[name] = 'undefined'
                else:
                    _deps[name] = 'defined'
                    get_function_dependencies(module, name, _deps=_deps)
    return _deps


def compile_to_LLVM(functions_and_signatures, target, server=None,
                    use_host_target=False, debug=False):
    """Compile functions with given signatures to target specific LLVM IR.

    Parameters
    ----------
    functions_and_signatures : list
      Specify a list of Python function and its signatures pairs.
    target : {'host', 'cuda', 'cuda32'}
      Specify LLVM IR target.
    server : {Server, None}
      Specify server containing the target. If unspecified, use local
      host. [NOT IMPLEMENTED]
    use_host_target: bool
      Use cpu target for constructing LLVM module and reset to
      specified target. This is useful when numba.cuda does not
      implement features that cpu target is supporting (like operator
      overloading, etc). Note that this is not guaranteed to work in
      general as there numba cpu target may generate instructions that
      NVVM does not support.

    Returns
    -------
    module : llvmlite.binding.ModuleRef
      LLVM module instance. To get the IR string, use `str(module)`.

    """
    if server is None or is_localhost(server.host):
        if triple_matches(target, 'host'):
            target_desc = nb.targets.registry.cpu_target
            typing_context = target_desc.typing_context
            target_context = target_desc.target_context
            use_host_target = False
        elif triple_matches(target, 'cuda'):
            if use_host_target:
                triple = 'nvptx64-nvidia-cuda'
                data_layout = nb.cuda.cudadrv.nvvm.data_layout[64]
                target_desc = nb.targets.registry.cpu_target
                typing_context = target_desc.typing_context
                target_context = target_desc.target_context
            else:
                target_desc = nb.cuda.descriptor.CUDATargetDesc
                typing_context = target_desc.typingctx
                target_context = target_desc.targetctx
        elif triple_matches(target, 'cuda32'):
            if use_host_target:
                triple = 'nvptx-nvidia-cuda'
                data_layout = nb.cuda.cudadrv.nvvm.data_layout[32]
                target_desc = nb.targets.registry.cpu_target
                typing_context = target_desc.typing_context
                target_context = target_desc.target_context
            else:
                target_desc = nb.cuda.descriptor.CUDATargetDesc
                typing_context = target_desc.typingctx
                target_context = target_desc.targetctx
        else:
            cpu_target = llvm.get_process_triple()
            raise NotImplementedError(repr((target, cpu_target)))
    else:
        raise NotImplementedError(repr((target, server)))

    codegen = target_context.codegen()
    main_library = codegen.create_library('rbc.irtools.compile_to_IR')
    main_module = main_library._final_module

    flags = nb.compiler.Flags()
    flags.set('no_compile')
    flags.set('no_cpython_wrapper')

    function_names = []
    for func, signatures in functions_and_signatures:
        for sig in signatures:
            fname = func.__name__ + sig.mangling
            function_names.append(fname)
            args, return_type = nb.sigutils.normalize_signature(
                sig.tonumba(bool_is_int8=True))
            cres = nb.compiler.compile_extra(typingctx=typing_context,
                                             targetctx=target_context,
                                             func=func,
                                             args=args,
                                             return_type=return_type,
                                             flags=flags,
                                             library=main_library,
                                             locals={})
            # The compilation result contains a function with
            # prototype `<function name>(<rtype>* result,
            # <arguments>)`. In the following we add a new function
            # with a prototype `<rtype> <fname>(<arguments>)`:
            fndesc = cres.fndesc
            module = cres.library.create_ir_module(fndesc.unique_name)
            context = cres.target_context
            ll_argtypes = [context.get_value_type(ty) for ty in args]
            ll_return_type = context.get_value_type(return_type)

            wrapty = ir.FunctionType(ll_return_type, ll_argtypes)
            wrapfn = module.add_function(wrapty, fname)
            builder = ir.IRBuilder(wrapfn.append_basic_block('entry'))
            fnty = context.call_conv.get_function_type(return_type, args)
            fn = builder.module.add_function(fnty, cres.fndesc.llvm_func_name)
            status, out = context.call_conv.call_function(
                builder, fn, return_type, args, wrapfn.args)
            builder.ret(out)
            cres.library.add_ir_module(module)

    seen = set()
    for _library in main_library._linking_libraries:
        if _library not in seen:
            seen.add(_library)
            main_module.link_in(
                _library._get_module_for_linking(), preserve=True,
            )

    main_library._optimize_final_module()

    # Catch undefined functions:
    used_functions = set(function_names)
    for fname in function_names:
        deps = get_function_dependencies(main_module, fname)
        for fn, descr in deps.items():
            used_functions.add(fn)
            if descr == 'undefined':
                raise RuntimeError('function `%s` is undefined' % (fn))

    # for global_variable in main_module.global_variables:
    #    global_variable.linkage = llvm.Linkage.private

    unused_functions = [f.name for f in main_module.functions
                        if f.name not in used_functions]

    if unused_functions:
        if debug:
            print('compile_to_IR: the following functions are not used'
                  ' and will be removed:')
        for fname in unused_functions:
            if debug:
                print('  ', fname)
            lf = main_module.get_function(fname)
            lf.linkage = llvm.Linkage.private
        main_library._optimize_final_module()
    # TODO: determine unused global_variables and struct_types

    main_module.verify()
    main_library._finalized = True

    if use_host_target:
        # when using reuse_host_target option for given target, we
        # assume that target triple and data_layout is defined above:
        main_module.triple = triple
        main_module.data_layout = data_layout

    return main_module


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

    Usage on host
    -------------

    To get the address of the compiled functions, use

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
