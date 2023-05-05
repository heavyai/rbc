"""Low-level intrinsics to expose external functions
"""

# This code has heavily inspired in the numba.extending.intrisic code
from typing import Dict, List, Union
from collections import defaultdict
from numba.core import extending, funcdesc, types, typing
from rbc.typesystem import Type
from rbc.targetinfo import TargetInfo


class External:
    @classmethod
    def external(cls, *args):
        """
        Parameters
        ----------
        signature : object (str, ctypes function, python callable, numba function)
            Any object convertible to a Numba function via Type.fromobject(...).tonumba()
        """
        all_possible_devices = {'CPU', 'GPU'}
        ts = defaultdict(list)
        key = None
        for signature in args:
            with TargetInfo.dummy():
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
            signature_devices = [a.upper() for a in (t.annotation() or [])
                                 if a.upper() in all_possible_devices]
            devices = signature_devices or all_possible_devices
            for device in devices:
                ts[device].append(signature)

        obj = cls(key, dict(ts))
        obj.register()
        return obj

    def __init__(
        self,
        key: str,
        signatures: Dict[str, List[Union[str, types.FunctionType, Type]]],
    ):
        """
        Parameters
        ----------
        key : str
            The key of the external function for typing
        signatures : Dictionary of devices and function signatures
            A device mapping of a list of function type signature
        """
        self._signatures = signatures
        self.key = key

    def __str__(self):
        a = []
        for device in self._signatures:
            for t in self._signatures[device]:
                a.append(str(t))
        return ", ".join(a)

    @property
    def name(self):
        return self.key

    def match_signature(self, atypes):
        # Code here is the same found in remotejit.py::Signature::best_match
        target_info = TargetInfo()
        device = "CPU" if target_info.is_cpu else "GPU"
        if device not in self._signatures:
            satypes = ", ".join(map(str, atypes))
            compile_target = target_info.get_compile_target()
            raise TypeError(
                f"{compile_target=}: no signatures with argument types `{satypes}` and"
                f" device `{device}` while processing `{self.name}`"
                " (perhaps the compile target cannot support the device?)"
            )
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
                f"{compile_target=}: found no matching function type to the given argument types"
                f" `{satypes}` and device `{device}` while processing `{self.name}`."
                f" Available function types: {available}"
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
                TargetInfo().add_external(t.name)

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


external = External.external
