import types as py_types
from rbc.targetinfo import TargetInfo
from rbc.typesystem import Type
from numba.core import funcdesc, typing
from numba.core.typing.templates import infer_global
from numba.core.imputils import lower_builtin


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


def register_external(
    fname,
    retty,
    argtys,
    module_name,
    module_globals,
    doc,
):

    # expose
    fn = eval(f'lambda {",".join(map(lambda x: sanitize(x.name), argtys))}: None', {}, {})
    _key = py_types.FunctionType(fn.__code__, {}, fname)
    _key.__module__ = __name__
    globals()[fname] = _key

    # typing
    @infer_global(_key)
    class ExternalTemplate(typing.templates.AbstractTemplate):
        key = _key

        def generic(self, args, kws):
            retty_ = Type.fromobject(retty).tonumba()
            argtys_ = tuple(map(lambda x: Type.fromobject(x.ty).tonumba(), argtys))
            codegen = gen_codegen(fname)
            lower_builtin(_key, *argtys_)(codegen)
            return retty_(*argtys_)

    module_globals[fname] = _key
    _key.__module__ = module_name
    _key.__doc__ = doc
    del globals()[fname]
    return _key


def make_intrinsic(fname, retty, argnames, module_name, module_globals, doc):
    argnames = tuple(map(lambda x: sanitize(x), argnames))
    fn_str = (
        f'from numba.core import types, funcdesc\n'
        f'from numba.core.extending import intrinsic\n'
        f'@intrinsic\n'
        f'def {fname}(typingctx, {", ".join(argnames)}):\n'
        f'    signature = types.{retty}({", ".join(argnames)})\n'
        f'    def codegen(context, builder, sig, args):\n'
        f'        fndesc = funcdesc.ExternalFunctionDescriptor("{fname}", sig.return_type, sig.args)\n'  # noqa: E501
        f'        func = context.declare_external_function(builder.module, fndesc)\n'
        f'        return builder.call(func, args)\n'
        f'    return signature, codegen'
    )

    exec(fn_str, locals())
    fn = locals()[fname]
    fn.__module__ = module_name
    fn.__doc__ = doc
    fn.__name__ = fname
    module_globals[fname] = fn
    return fn
