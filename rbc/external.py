# This code has heavily inspired in the numba.extending.intrisic code

from numba.core import extending, funcdesc, types, typing
from rbc.typesystem import Type
from rbc.targetinfo import TargetInfo


class External:
    @classmethod
    def fromobject(cls, signature, name: str = None):
        """
        Parameters
        ----------
        signature : object (str, ctypes function, python callable, numba function)
            Any object convertible to a Numba function via Type.fromobject(...).tonumba()
        name : str
            The name of the external function
        """
        # Make inner function for the actual work
        target_info = TargetInfo.host()
        with target_info:
            t = Type.fromobject(signature)
            if not t.is_function:
                raise ValueError("signature must represent a function type")

            if name is None:
                name = t.name
            if not name:
                raise ValueError(
                    f"external function name not specified for signature {signature}"
                )

            typ = t
            t = t.tonumba()

        return cls(name, t, typ)

    def __init__(self, name: str, signature: types.FunctionType, typ: Type):
        """
        Parameters
        ----------
        name : str
            The name of the external function
        signature : Numba function signature
            A numba function type signature. i.e. (float64, float64) -> int64
        rbc_type: RBC typesystem Type
            A type from the RBC typesystem type
        """
        self._signature = signature
        self.rbc_typ = typ
        self.name = name
        self.register()

    def register(self):
        # typing
        class ExternalTemplate(typing.templates.AbstractTemplate):
            obj = self
            key = self.name

            def generic(self, args, kws):
                t = Type.fromobject(self.obj.rbc_typ).tonumba()

                name = self.key

                # lowering
                def codegen(context, builder, sig, args):
                    fndesc = funcdesc.ExternalFunctionDescriptor(
                        name, sig.return_type, sig.args
                    )
                    func = context.declare_external_function(builder.module, fndesc)
                    return builder.call(func, args)

                extending.lower_builtin(name, *t.args)(codegen)
                return t

        typing.templates.infer(ExternalTemplate)
        typing.templates.infer_global(self, types.Function(ExternalTemplate))

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
