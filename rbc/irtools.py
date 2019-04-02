# Author: Pearu Peterson
# Created: February 2019

import re
import inspect
import numba as nb
from llvmlite import ir
import llvmlite.binding as llvm


def initialize_llvm():
    llvm.initialize()
    llvm.initialize_all_targets()
    llvm.initialize_all_asmprinters()


def compile_to_IR(functions_and_signatures, target, server=None):
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

    Returns
    -------
    ir : str
      LLVM IR string for the given target.
    """
    cpu_target = llvm.get_process_triple()
    if server is None or server.host in ['localhost', '127.0.0.1']:
        if target == 'host' or target == cpu_target:
            # FYI, there is also get_process_triple()
            # triple = llvm.get_default_triple()
            target_desc = nb.targets.registry.cpu_target
            typing_context = target_desc.typing_context
            target_context = target_desc.target_context
        elif target == 'cuda' or target == 'nvptx64-nvidia-cuda':
            # triple = 'nvptx64-nvidia-cuda'
            target_desc = nb.cuda.descriptor.CUDATargetDesc
            typing_context = target_desc.typingctx
            target_context = target_desc.targetctx
        elif target == 'cuda32' or target == 'nvptx-nvidia-cuda':
            # triple = 'nvptx-nvidia-cuda'
            target_desc = nb.cuda.descriptor.CUDATargetDesc
            typing_context = target_desc.typingctx
            target_context = target_desc.targetctx
        else:
            raise NotImplementedError(repr((target, cpu_target)))
    else:
        raise NotImplementedError(repr((target, server)))
    flags = nb.compiler.Flags()
    flags.set('no_compile')
    flags.set('no_cpython_wrapper')

    main_mod = llvm.parse_assembly('source_filename="{}"'.format(__file__))
    main_mod.name = 'rbc_irtools'

    for func, signatures in functions_and_signatures:
        for sig in signatures:
            fname = func.__name__ + sig.mangling
            args, return_type = nb.sigutils.normalize_signature(sig.tonumba())
            cres = nb.compiler.compile_extra(typingctx=typing_context,
                                             targetctx=target_context,
                                             func=func,
                                             args=args,
                                             return_type=return_type,
                                             flags=flags,
                                             locals={})
            # C wrapper
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

            cres.library._optimize_final_module()
            cres.library._final_module.verify()
            cres.library._finalized = True
            llvmir = cres.library.get_llvm_str()
            main_mod.link_in(llvm.parse_assembly(llvmir), preserve=True)

    main_mod.verify()
    irstr = str(main_mod)
    return irstr


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
