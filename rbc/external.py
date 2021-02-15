# This code has heavily inspired in the numba.extending.intrisic code
from typing import Dict, List
from collections import defaultdict
from numba.core import extending, funcdesc, types, typing
from rbc.typesystem import Type
from rbc.targetinfo import TargetInfo


class External:
    @classmethod
    def fromobject(cls, *args, typing=True, lowering=True):
        """
        Parameters
        ----------
        signature : object (str, ctypes function, python callable, numba function)
            Any object convertible to a Numba function via Type.fromobject(...).tonumba()
        name : str
            The name of the external function
        typing : bool
            Indicates if External should do typing or not. Default is True
        lowering: bool
            Indicates if External should do lowering or not. Default is True. Except for
            very specific cases, typing and lowering should be True
        """
        # Make inner function for the actual work
        target_info = TargetInfo.dummy()
        ts = defaultdict(list)
        name = None
        with target_info:
            for signature in args:
                t = Type.fromobject(signature)
                if not t.is_function:
                    raise ValueError("signature must represent a function type")

                if not t.name:
                    raise ValueError(
                        f"external function name not specified for signature {signature}"
                    )

                if name is None:
                    name = t.name
                if not name:
                    raise ValueError(
                        f"external function name not specified for signature {signature}"
                    )

                if t.annotation():
                    device = "CPU" if "CPU" in t.annotation() else "GPU"
                    ts[device].append(t)
                else:
                    ts["CPU"].append(t)
                    ts["GPU"].append(t)

        return cls(name, ts, typing=typing, lowering=lowering)

    def __init__(
        self,
        name: str,
        signatures: Dict[str, List[types.FunctionType]],
        typing=True,
        lowering=True,
    ):
        """
        Parameters
        ----------
        name : str
            The name of the external function
        signature : Numba function signature
            A numba function type signature. i.e. (float64, float64) -> int64
        """
        self._signatures = signatures
        self.name = name  # this name refers to the one used as a key to the template
        # the actual function name we get from the signature
        self.typing = typing
        self.lowering = lowering
        if self.typing:
            self.register()

    def __str__(self):
        a = []
        for device in self._signatures:
            for t in self._signatures[device]:
                a.append(t.tostring())
        return ", ".join(a)

    def match_signature(self, atypes):
        # Code here is the same found in remotejit.py::Signature::best_match
        device = "CPU" if TargetInfo().is_cpu else "GPU"
        available_types = self._signatures[device]
        ftype = None
        match_penalty = None
        atypes = tuple(map(Type.fromobject, atypes))
        for typ in available_types:
            penalty = typ.match(atypes)
            if penalty is not None:
                if ftype is None or penalty < match_penalty:
                    ftype = typ
                    match_penalty = penalty
        if ftype is None:
            satypes = ", ".join(map(str, atypes))
            available = "; ".join(map(str, available_types))
            raise TypeError(
                f"found no matching function type to given argument types"
                f" `{satypes}`. Available function types: {available}"
            )
        return ftype

    def get_codegen(self):
        # lowering
        def codegen(context, builder, sig, args):
            # Need to retrieve the function name again
            t = self.match_signature(sig.args)
            fn_name = t.name
            fndesc = funcdesc.ExternalFunctionDescriptor(
                fn_name, sig.return_type, sig.args
            )
            func = context.declare_external_function(builder.module, fndesc)
            return builder.call(func, args)

        return codegen

    def register(self):
        # typing
        class ExternalTemplate(typing.templates.AbstractTemplate):
            obj = self
            key = self.name

            def generic(self, args, kws):
                # get the correct signature and function name for the current device
                t = self.obj.match_signature(args)

                if self.obj.lowering:
                    codegen = self.obj.get_codegen()
                    extending.lower_builtin(self.key, *t.tonumba().args)(codegen)
                return t.tonumba()

        typing.templates.infer(ExternalTemplate)
        typing.templates.infer_global(self, types.Function(ExternalTemplate))

    def __call__(self, *args, **kwargs):
        """
        This is only defined to pretend to be a callable from CPython.
        """
        msg = f"{self.name} is not usable in pure-python"
        raise NotImplementedError(msg)


external = External.fromobject
