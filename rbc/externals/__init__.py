from rbc.targetinfo import TargetInfo
from numba.core import funcdesc


def gen_codegen(fn_name):
    if fn_name.startswith('llvm.'):
        def codegen(context, builder, sig, args):
            func = builder.module.declare_intrinsic(fn_name, [a.type for a in args])
            return builder.call(func, args)
    else:
        def codegen(context, builder, sig, args):
            fndesc = funcdesc.ExternalFunctionDescriptor(fn_name, sig.return_type, sig.args)
            func = context.declare_external_function(builder.module, fndesc)
            return builder.call(func, args)

    return codegen


def dispatch_codegen(cpu, gpu):
    def inner(context, builder, sig, args):
        impl = cpu if TargetInfo().is_cpu else gpu
        return impl(context, builder, sig, args)

    return inner


def sanitize(name):
    forbidden_names = ('in')
    if name in forbidden_names:
        return f"{name}_"
    return name


def make_intrinsic(fname, retty, argnames, module_name, module_globals, doc):
    argnames = tuple(map(lambda x: sanitize(x), argnames))
    fn_str = f'''
from numba.core.extending import intrinsic
@intrinsic
def {fname}(typingctx, {", ".join(argnames)}):
    from rbc.typesystem import Type
    retty_ = Type.fromobject("{retty}").tonumba()
    argnames_ = tuple(map(lambda x: Type.fromobject(x).tonumba(), [{", ".join(argnames)}]))  # noqa: E501
    signature = retty_(*argnames_)
    def codegen(context, builder, sig, args):
        from numba.core import funcdesc
        fndesc = funcdesc.ExternalFunctionDescriptor("{fname}", sig.return_type, sig.args)  # noqa: E501
        func = context.declare_external_function(builder.module, fndesc)
        return builder.call(func, args)
    return signature, codegen
'''

    exec(fn_str, module_globals)
    fn = module_globals[fname]
    fn.__doc__ = doc
    return fn
