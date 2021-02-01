# This code has heavily inspired in the numba.extending.intrisic code

from numba.core import extending, funcdesc, types
from rbc.typesystem import Type


class _External:
    def __init__(self, symbol, signature):
        self.symbol = symbol
        self._signature = signature

    def register(self):
        # typing
        from numba.core.typing.templates import (
            make_concrete_template,
            infer_global,
            infer,
        )

        template = make_concrete_template(
            self.symbol, key=self.symbol, signatures=[self.signature]
        )
        infer(template)
        infer_global(self, types.Function(template))

        # lowering
        def codegen(context, builder, sig, args):
            fndesc = funcdesc.ExternalFunctionDescriptor(
                self.symbol, sig.return_type, sig.args
            )
            func = context.declare_external_function(builder.module, fndesc)
            return builder.call(func, args)

        extending.lower_builtin(self.symbol, *self.signature.args)(codegen)

    def __call__(self, *args, **kwargs):
        """
        This is only defined to pretend to be a callable from CPython.
        """
        msg = f"{self.symbol} is not usable in pure-python"
        raise NotImplementedError(msg)

    @property
    def return_type(self):
        return self.signature.return_type

    @property
    def args(self):
        return self.signature.args

    @property
    def signature(self):
        return self._signature


def external(signature, name=None):

    # Make inner function for the actual work
    t = Type.fromobject(signature)
    if not t.is_function:
        raise ValueError("signature must represent a function type")

    if name is None:
        name = t.name
    if not name:
        raise ValueError(
            f"external function name not specified for signature {signature}"
        )

    llc = _External(name, t.tonumba())
    llc.register()
    return llc
