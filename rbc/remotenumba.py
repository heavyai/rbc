"""
Requires:
  llvmdev-7.0.1 with all targets enabled

"""

import re
import inspect
from llvmlite import ir
import llvmlite.binding as llvm
import numba as nb
import numba.sigutils
import numba.cuda


def initialize_llvm():
    llvm.initialize()
    llvm.initialize_all_targets()
    llvm.initialize_all_asmprinters()


initialize_llvm()


def annotation_to_numba_type(a):
    """Return numba type from a signature annotation.
    """
    if isinstance(a, str):
        if a.endswith('*'):
            return nb.types.CPointer(annotation_to_numba_type(a[:-1].strip()))
        t = nb.types.__dict__.get(a, None)
        if isinstance(t, nb.types.Type):
            return t
    elif a is inspect.Signature.empty or a is None:
        return nb.types.void
    elif a is int:
        return nb.types.int_                      # int64
    elif a is float:
        return nb.types.double
    elif a is complex:
        return nb.types.complex128
    elif a is bytes:
        return nb.types.CPointer(nb.types.byte)   # uint8*
    elif a is str:
        return nb.types.string                    # unicode_type
    raise NotImplementedError(
        f'converting {a} of type {type(a)} to numba types')


def get_numba_signature(func, return_type=None):
    """Return numba signature from a function.
    """
    s = inspect.signature(func)
    argtypes = []
    for i, a in enumerate(s.parameters):
        t = annotation_to_numba_type(s.parameters[a].annotation)
        if t == nb.types.void:
            print(f'Warning: {i+1}-th parameter `{a}` in `{func.__name__}`'
                  'does not define type. Assuming `int32`')
            t = nb.types.int32
        argtypes.append(t)
    if return_type is None:
        rtype = s.return_annotation
    else:
        rtype = return_type
    rtype = annotation_to_numba_type(rtype)
    # TODO: warn if return annotation is missing but func contains
    # return statement
    return rtype(*argtypes)


def get_llvm_ir(func, sig=None,
                target='host',
                locals={}, options={}):
    """Return LLVM IR of a Python function.

    Parameters
    ----------
    func : callable
      Specify Python function.
    sig : {<numba signature>, dict}
      Specify the numba signature of the Python function.
    target : {'host', 'cuda', llvmlite.binding.Target}
      Specify IR target.
    """
    if sig is None:
        sig = get_numba_signature(func)

    if target == 'host':
        # triple = llvm.get_default_triple()
        # there is also get_process_triple
        target_desc = nb.targets.registry.cpu_target
        typing_context = target_desc.typing_context
        target_context = target_desc.target_context
    elif target == 'cuda':
        # triple = 'nvptx64-nvidia-cuda'
        target_desc = nb.cuda.descriptor.CUDATargetDesc
        typing_context = target_desc.typingctx
        target_context = target_desc.targetctx
    else:
        raise NotImplementedError(repr(target))
    flags = nb.compiler.Flags()
    flags.set('no_compile')
    flags.set('no_cpython_wrapper')

    if not isinstance(sig, dict):
        sig = {func.__name__: sig}

    main_mod = llvm.parse_assembly('source_filename="{}"'
                                   .format(inspect.getsourcefile(func)))
    main_mod.name = func.__name__

    for fname, _sig in sig.items():
        args, return_type = nb.sigutils.normalize_signature(_sig)
        cres = nb.compiler.compile_extra(typingctx=typing_context,
                                         targetctx=target_context,
                                         func=func,
                                         args=args,
                                         return_type=return_type,
                                         flags=flags,
                                         locals=locals)
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
    # todo: return AST as well
    return str(main_mod)


def get_triple(llvm_ir):
    return re.search(r'target\s+triple\s*=\s*"(?P<triple>[-\d\w\W_]+)"\s*$',
                     llvm_ir, re.M).group('triple')


def create_execution_engine(triple):
    target = llvm.Target.from_triple(triple)
    target_machine = target.create_target_machine()
    backing_mod = llvm.parse_assembly("")
    engine = llvm.create_mcjit_compiler(backing_mod, target_machine)
    return engine


def compile_ir(engine, llvm_ir):
    # Create a LLVM module object from the IR
    mod = llvm.parse_assembly(llvm_ir)
    mod.verify()
    engine.add_module(mod)
    engine.finalize_object()
    engine.run_static_constructors()
    return mod


def compile(llvm_ir):
    engine = create_execution_engine(get_triple(llvm_ir))
    compile_ir(engine, llvm_ir)
    return engine
