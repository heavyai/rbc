from rbc.external import declare
from rbc.typesystem import Type
from numba.core import imputils, typing
from numba.cuda import libdevicefuncs

# Typing
typing_registry = typing.templates.Registry()
infer_global = typing_registry.register_global

# Lowering
lowering_registry = imputils.Registry()
lower = lowering_registry.lower


def register(fname, retty, argtys):

    # expose
    s = f"def {fname}(*args): pass"
    exec(s, globals())
    _key = globals()[fname]

    # typing
    @infer_global(_key)
    class LibDeviceTemplate(typing.templates.AbstractTemplate):
        key = _key

        def generic(self, args, kws):
            # get the correct signature and function name for the current device
            atypes = tuple(map(Type.fromobject, args))
            e = declare(f"{retty} {fname}({', '.join(argtys)})|GPU")

            t = e.match_signature(atypes)

            codegen = e.get_codegen()
            lower(_key, *t.tonumba().args)(codegen)

            return t.tonumba()


for fname, (retty, args) in libdevicefuncs.functions.items():
    argtys = tuple(map(lambda x: f"{x.ty}*" if x.is_ptr else f"{x.ty}", args))
    register(fname, retty, argtys)
