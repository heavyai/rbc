from rbc.external import external
from rbc.typesystem import Type
from numba.core import imputils, typing
from numba.cuda import libdevicefuncs

# Typing
typing_registry = typing.templates.Registry()
infer_global = typing_registry.register_global

# Lowering
lowering_registry = imputils.Registry()
lower = lowering_registry.lower


for fname, (retty, args) in libdevicefuncs.functions.items():
    argtys = tuple(map(lambda x: f"{x.ty}*" if x.is_ptr else f"{x.ty}", args))
    t = Type.fromstring(f"{retty} {fname}({', '.join(argtys)})")

    # expose
    s = f"def {fname}(*args): pass"
    exec(s, globals())
    key = globals()[fname]

    # typing
    class CmathTemplate(typing.templates.ConcreteTemplate):
        cases = [t.tonumba()]

    infer_global(key)(CmathTemplate)

    # lowering
    e = external(
        f"{retty} {fname}({', '.join(argtys)})|GPU", typing=False, lowering=False
    )
    print(e)
    lower(key, *t.tonumba().args)(e.get_codegen())
