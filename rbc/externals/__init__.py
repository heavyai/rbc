import types as py_types
from rbc.targetinfo import TargetInfo
from rbc.typesystem import Type
from numba.core import funcdesc, typing


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


forbidden_names = ('in')
def sanitize(name):
    if name in forbidden_names:
        return f"{name}_"
    return name

def register_external(
    fname,
    retty,
    argtys,
    module_name,
    module_globals,
    typing_registry,
    lowering_registry,
    doc,
):

    # expose
    fn = eval(f'lambda {",".join(map(lambda x: sanitize(x.name), argtys))}: None', {}, {})
    _key = py_types.FunctionType(fn.__code__, {}, fname)
    _key.__module__ = __name__
    globals()[fname] = _key

    # typing
    @typing_registry.register_global(_key)
    class ExternalTemplate(typing.templates.AbstractTemplate):
        key = _key

        def generic(self, args, kws):
            # get the correct signature and function name for the current device
            t = Type.fromstring(f"{retty} {fname}({', '.join(argtys)})")
            codegen = gen_codegen(fname)
            lowering_registry.lower(_key, *t.tonumba().args)(codegen)

            return t.tonumba()

    module_globals[fname] = _key
    _key.__module__ = module_name
    _key.__doc__ = doc
    del globals()[fname]
    return _key
