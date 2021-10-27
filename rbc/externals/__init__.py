from rbc.targetinfo import TargetInfo
from numba.core import funcdesc


def gen_codegen(fn_name):
    def codegen(context, builder, sig, args):
        # Need to retrieve the function name again
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
    fn_str = (
        f'from numba.core import funcdesc\n'
        f'from numba.core.extending import intrinsic\n'
        f'from rbc.typesystem import Type\n'
        f'from rbc.targetinfo import TargetInfo\n'
        f'@intrinsic\n'
        f'def {fname}(typingctx, {", ".join(argnames)}):\n'
        f'    retty_ = Type.fromobject("{retty}").tonumba()\n'
        f'    argnames_ = tuple(map(lambda x: Type.fromobject(x).tonumba(), [{", ".join(argnames)}]))\n'  # noqa: E501
        f'    signature = retty_(*argnames_)\n'
        f'    def codegen(context, builder, sig, args):\n'
        f'        fndesc = funcdesc.ExternalFunctionDescriptor("{fname}", sig.return_type, sig.args)\n'  # noqa: E501
        f'        func = context.declare_external_function(builder.module, fndesc)\n'
        f'        return builder.call(func, args)\n'
        f'    return signature, codegen'
    )

    exec(fn_str, globals(), locals())
    fn = locals()[fname]
    fn.__module__ = module_name
    fn.__doc__ = doc
    fn.__name__ = fname
    module_globals[fname] = fn
    return fn
