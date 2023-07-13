import inspect
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

    # https://chriswarrick.com/blog/2018/09/20/python-hackery-merging-signatures-of-two-python-functions/
    # this is to remove `typingctx` from showing up in the docs
    sig = inspect.signature(fn)
    assert list(sig.parameters.keys())[0] == 'typingctx'
    fn.__signature__ = sig.replace(parameters=tuple(sig.parameters.values())[1:])
    return fn
