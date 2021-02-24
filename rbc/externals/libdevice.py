from rbc.externals import utils
from rbc.typesystem import Type
from rbc.utils import get_version

assert get_version("numba") >= (0, 52)
from numba.core import imputils, typing  # noqa: E402
from numba.cuda import libdevicefuncs  # noqa: E402

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
            t = Type.fromstring(f"{retty} {fname}({', '.join(argtys)})")
            codegen = utils.gen_codegen(fname)
            lower(_key, *t.tonumba().args)(codegen)

            return t.tonumba()


for fname, (retty, args) in libdevicefuncs.functions.items():
    argtys = tuple(map(lambda x: f"{x.ty}*" if x.is_ptr else f"{x.ty}", args))
    register(fname, retty, argtys)
