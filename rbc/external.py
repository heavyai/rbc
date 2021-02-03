# This code has heavily inspired in the numba.extending.intrisic code

from numba.core import extending, funcdesc, types
from rbc.typesystem import Type


class External:
    @classmethod
    def fromobject(cls, signature, name: str = None):
        """
        Parameters
        ----------
        signature : Numba function signature
            A numba function type signature. i.e. (float64, float64) -> int64
        name : str
            The name of the external function
        """
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

        return cls(name, t.tonumba())

    def __init__(self, name: str, signature: types.FunctionType):
        """
        Parameters
        ----------
        name : str
            The name of the external function
        signature : Numba function signature
            A numba function type signature. i.e. (float64, float64) -> int64
        """
        self._signature = signature
        self.name = name
        self.register()

    def register(self):
        # typing
        from numba.core.typing.templates import (
            make_concrete_template,
            infer_global,
            infer,
        )

        template = make_concrete_template(
            self.name, key=self.name, signatures=[self.signature]
        )
        infer(template)
        infer_global(self, types.Function(template))

        # lowering
        def codegen(context, builder, sig, args):
            fndesc = funcdesc.ExternalFunctionDescriptor(
                self.name, sig.return_type, sig.args
            )
            func = context.declare_external_function(builder.module, fndesc)
            return builder.call(func, args)

        extending.lower_builtin(self.name, *self.signature.args)(codegen)

    def __call__(self, *args, **kwargs):
        """
        This is only defined to pretend to be a callable from CPython.
        """
        msg = f"{self.name} is not usable in pure-python"
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


external = External.fromobject
