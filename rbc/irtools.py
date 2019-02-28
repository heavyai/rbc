# Author: Pearu Peterson
# Created: February 2019

import inspect
import numba as nb
from llvmlite import ir
import llvmlite.binding as llvm


def initialize_llvm():
    llvm.initialize()
    llvm.initialize_all_targets()
    llvm.initialize_all_asmprinters()


def compile_function_to_IR(func, signatures, target, server=None):
    """Parameters
    ----------
    func : function
      Specify Python function.
    signatures : list
      Specify a list of function types (`Type` instances) for which IR
      functions will be compiled.
    target : {'host', 'cuda', 'cuda32'}
      Specify LLVM IR target.
    server : {Server, None}
      Specify server containing the target. If unspecified, use local
      host.

    Returns
    -------
    ir : str
      LLVM IR string for the given target.

    """
    initialize_llvm()
    if server is None or server.host == 'localhost':
        if target == 'host':
            # FYI, there is also get_process_triple()
            # triple = llvm.get_default_triple()
            target_desc = nb.targets.registry.cpu_target
            typing_context = target_desc.typing_context
            target_context = target_desc.target_context
        elif target == 'cuda':
            # triple = 'nvptx64-nvidia-cuda'
            target_desc = nb.cuda.descriptor.CUDATargetDesc
            typing_context = target_desc.typingctx
            target_context = target_desc.targetctx
        elif target == 'cuda32':
            # triple = 'nvptx-nvidia-cuda'
            target_desc = nb.cuda.descriptor.CUDATargetDesc
            typing_context = target_desc.typingctx
            target_context = target_desc.targetctx
        else:
            raise NotImplementedError(repr(target))
    else:
        raise NotImplementedError(repr((target, server)))
    flags = nb.compiler.Flags()
    flags.set('no_compile')
    flags.set('no_cpython_wrapper')

    main_mod = llvm.parse_assembly('source_filename="{}"'
                                   .format(inspect.getsourcefile(func)))
    main_mod.name = func.__name__

    for sig in signatures:
        fname = func.__name__ + sig.mangle()
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

    return str(main_mod)
