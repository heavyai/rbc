# This code has heavily inspired in the numba.extending.intrisic code

from numba.core import extending, funcdesc, types
from rbc.typesystem import Type


class _External:
    def __init__(self, symbol, retty, argtys):
        self.symbol = symbol
        self.signature = retty(*argtys)

    def _type_infer(self):
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

    def _lower_external_call(self):
        def codegen(context, builder, sig, args):
            fndesc = funcdesc.ExternalFunctionDescriptor(
                self.symbol, sig.return_type, sig.args
            )
            func = context.declare_external_function(builder.module, fndesc)
            return builder.call(func, args)

        extending.lower_builtin(self.symbol, *self.signature.args)(codegen)

    def register(self):
        self._type_infer()
        self._lower_external_call()

    def __call__(self, *args, **kwargs):
        """
        This is only defined to pretend to be a callable from CPython.
        """
        msg = f'{self.symbol} is not usable in pure-python'
        raise NotImplementedError(msg)


def external(*args):

    # Make inner function for the actual work
    t = Type.fromstring(args[0])
    if t.is_function:
        name = t.name
        retty = t[0].tonumba()
        argtys = tuple(map(lambda x: x.tonumba(), t[1]))

        llc = _External(name, retty, argtys)
        llc.register()
        return llc
    else:
        raise ValueError
