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
        typing : bool
            Indicates if External should do typing or not. Default is True
        lowering: bool
            Indicates if External should do lowering or not. Default is True. Except for
            very specific cases, typing and lowering should be True
        """
        # Make inner function for the actual work
        target_info = TargetInfo.dummy()
        ts = defaultdict(list)
        key = None
        with target_info:
            for signature in args:
                t = Type.fromobject(signature)
                if not t.is_function:
                    raise ValueError("signature must represent a function type")

                if not t.name:
                    raise ValueError(
                        f"external function name not specified for signature {signature}"
                    )

                if key is None:
                    key = t.name
                if not key:
                    raise ValueError(
                        f"external function name not specified for signature {signature}"
                    )

                for device in [
                    a for a in t.annotation() or [] if a in ["CPU", "GPU"]
                ] or ["CPU", "GPU"]:
                    ts[device].append(signature)

        return cls(key, ts, typing=typing, lowering=lowering)

    def __init__(
        self,
        key: str,
        signatures: Dict[str, List[str]],
        typing=True,
        lowering=True,
    ):
        """
        Parameters
        ----------
        key : str
            The key of the external function for typing
        signatures : List of function signatures
            A list of function type signature. i.e. 'int64 fn(int64, float64)'
        typing : bool
            Indicates if External should do typing or not. Default is True
        lowering: bool
            Indicates if External should do lowering or not. Default is True. Except for
            very specific cases, typing and lowering should be True
        """
        self._signatures = signatures
        self.key = key
        self.typing = typing
        self.lowering = lowering
        if self.typing:
            self.register()

    def __str__(self):
        a = []
        for device in self._signatures:
            for t in self._signatures[device]:
                a.append(str(t))
        return ", ".join(a)

    def match_signature(self, atypes):
        # Code here is the same found in remotejit.py::Signature::best_match
        device = "CPU" if TargetInfo().is_cpu else "GPU"
        available_types = tuple(map(Type.fromobject, self._signatures[device]))
        ftype = None
        match_penalty = None
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
            atypes = tuple(map(Type.fromobject, sig.args))
            t = self.match_signature(atypes)
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
            key = self.key

            def generic(self, args, kws):
                # get the correct signature and function name for the current device
                atypes = tuple(map(Type.fromobject, args))
                t = self.obj.match_signature(atypes)

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
