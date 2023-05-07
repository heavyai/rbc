"""Low-level intrinsics to expose external functions
"""

# This code has heavily inspired in the numba.extending.intrisic code
import warnings
from typing import Dict, List, Union
from collections import defaultdict
from numba.core import extending, funcdesc, types, typing
from rbc.typesystem import Type
from rbc.targetinfo import TargetInfo
from rbc.errors import UnsupportedError
from rbc.utils import validate_devices


class External:

    @classmethod
    def external(cls, *args, **kwargs):
        """Parameters
        ----------
        signature : object (str, ctypes function, python callable, numba function)
            Any object convertible to a Numba function via Type.fromobject(...).tonumba()

        Keyword parameters
        ------------------
        devices : list
          Device names ('CPU' or/and 'GPU') for the given set of
          signatures. Default is ['CPU'].
        """
        if not args:
            raise ValueError('specifying at least one signature is required')

        # Default devices contains `CPU` only because in most cases
        # the externals are defined for the CPU target.
        devices = validate_devices(kwargs.get('devices', ['CPU']))

        ts = defaultdict(list)
        name = None
        for signature in args:
            with TargetInfo.dummy():
                t = Type.fromobject(signature)
            if not t.is_function:
                raise ValueError("signature must represent a function type")
            if not t.name:
                raise ValueError(
                    f"external function name not specified for signature {signature}"
                )
            if name is None:
                name = t.name
            elif name != t.name:
                raise ValueError('expected external function name `{name}`, got `{t.name}`')

            # Using annotation devices (specified via `| CPU` or `| GPU`) is deprecated.
            # TODO: turn the warning to an exception for rbc version 0.12 or higher
            annotation_devices = validate_devices([d for d in t.annotation()
                                                   if d.upper() in {'CPU', 'GPU'}])
            for d in annotation_devices:
                warnings.warn(
                    f'Signature {signature} uses deprecated device annotation (`|{d}`).'
                    ' Use `devices` kw argument to specify the supported devices of the'
                    f' external function `{name}`')
            signature_devices = annotation_devices or devices

            for device in signature_devices:
                ts[device].append(signature)

        # The key must contain devices bit to avoid spurious errors
        # when externals with the same name but variable device
        # targets are defined (as exemplified in
        # tests/heavydb/test_array.py:test_ptr):
        key = f'{name}-{"-".join(sorted(devices))}'
        obj = cls(key, name, dict(ts))
        obj.register()
        return obj

    def __init__(
        self,
        key: str,
        name: str,
        signatures: Dict[str, List[Union[str, types.FunctionType, Type]]],
    ):
        """
        Parameters
        ----------
        key : str
            The key of the external function for typing
        name : str
            The name of the external function
        signatures : Dictionary of devices and function signatures
            A device mapping of a list of function type signature
        """
        self._signatures = signatures
        self.key = key
        self.name = name

    def __str__(self):
        a = []
        for device in self._signatures:
            for t in self._signatures[device]:
                a.append(str(t))
        return ", ".join(a)

    def match_signature(self, atypes):
        # Code here is the same found in remotejit.py::Signature::best_match
        target_info = TargetInfo()
        device = "CPU" if target_info.is_cpu else "GPU"
        if device not in self._signatures:
            compile_target = target_info.get_compile_target()
            # Raising UnsupportedError will skip compiling the compile
            # target for the given device. This could also be skipped
            # by specifying the desired device in the devices kw
            # argument of a RemoteJIT decorator.
            if device == 'GPU':
                raise UnsupportedError(
                    f'no {device} specific signatures found for the external function'
                    f' `{self.name}` that is referenced by `{compile_target}`.'
                    f' Use devices=["CPU"] when defining `{compile_target.split("__", 1)[0]}`')
            else:
                raise UnsupportedError(
                    f'no {device} specific signatures found for the external function'
                    f' `{self.name}` that is referenced by `{compile_target}`.')

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
            # Need to retrieve the function name again as the function
            # implementation may be device specific
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
        def generic(self, args, kws):
            # get the correct signature and function name for the
            # current device
            atypes = tuple(map(Type.fromobject, args))
            t = self.obj.match_signature(atypes)
            TargetInfo().add_external(t.name)

            codegen = self.obj.get_codegen()
            t_numba = t.tonumba()
            extending.lower_builtin(self.key, *t_numba.args)(codegen)
            return t_numba

        ExternalTemplate = type(f"ExternalTemplate_{self.name}",
                                (typing.templates.AbstractTemplate,),
                                dict(generic=generic,
                                     obj=self,
                                     key=self.key))

        typing.templates.infer(ExternalTemplate)
        typing.templates.infer_global(self, types.Function(ExternalTemplate))

    def __call__(self, *args, **kwargs):
        """
        This is only defined to pretend to be a callable from CPython.
        """
        msg = f"{self.name} is not usable in pure-python"
        raise NotImplementedError(msg)


external = External.external
