# Author: Pearu Peterson
# Created: February 2019

import re
import inspect
import warnings
from .typesystem import Type
from . import irtools
from . import remotejit


class Caller(object):
    """Remote JIT caller
    """

    def __init__(self, remotejit, signatures, func,
                 local=False):
        """Construct remote JIT caller instance.

        Parameters
        ----------
        remotejit : RemoteJIT
          Specify remote JIT instance that contains remote server host
          and port information.
        signatures : list
          Specify a list of signatures (`Type` instances of function kind)
        func : callable
          Specify Python function as the template to remotely JIT
          compiled functions.
        local : bool
          When True, local process will be interpreted as
          remote. Useful for debugging.
        """
        self.remotejit = remotejit
        self._signatures = []
        self.func = func
        self.current_target = 'host'

        signature = inspect.signature(self.func)
        self.nargs = len(signature.parameters)

        for sig in signatures:
            self.add_signature(sig)

        self._client = None
        self.ir_cache = {}

        self.local = local

    def __repr__(self):
        return '%s(%s, %s, %s)' % (type(self).__name__, self.remotejit,
                                   [s.tostring() for s in self._signatures],
                                   self.func)

    def add_signature(self, sig):
        """Update Caller with a new signature
        """
        if not isinstance(sig, Type):
            sig = Type.fromobject(sig)
        nargs = self.nargs
        if not sig.is_function:
            raise ValueError(
                'expected signature with function kind, got `%s`' % (sig,))
        if nargs != len(sig[1]) and not (nargs == 0 and sig[1][0].is_void):
            raise ValueError(
                'mismatch of the number of arguments:'
                ' function %s, signature %s (`%s`)'
                % (nargs, len(sig[1]), sig))
        if sig not in self._signatures:
            self._signatures.append(sig)
        else:
            warnings.warn('Caller.add_signature:'
                          ' signature `%s` has been already added' % (sig,))

    def target(self, target=None):
        """Return current target. When specified, set a new target.
        """
        old_target = self.current_target
        if target is not None:
            self.current_target = target
        return old_target

    def get_IR(self, signatures=None):
        """Return LLVM IR string of compiled function for the current target.

        Deprecated, use compile_to_IR instead.
        """
        if signatures is None:
            signatures = self._signatures
        return irtools.compile_function_to_IR(self.func, signatures,
                                              self.current_target,
                                              self.remotejit)

    def compile_to_IR(self, signatures=None, targets=None):
        """Return a map of target triples and the corresponding LLVM IR
        strings.
        """
        if targets is None:
            targets = [self.current_target]
        triple_ir_map = {}
        for target in targets:
            old_target = self.target(target)
            ir = self.get_IR(signatures=signatures)
            self.target(old_target)
            triple = re.search(r'target\s+triple\s*=\s*"(?P<triple>.*?)"', ir)
            if triple is not None:
                triple_ir_map[triple.group('triple')] = ir
            else:
                raise RuntimeError(
                    'Caller.compile_to_IR: no `target triple = ...` in IR?!?')
        return triple_ir_map

    @property
    def client(self):
        if self._client is None:
            if self.local:
                self._client = remotejit.LocalClient()
            else:
                self._client = self.remotejit.make_client()
        return self._client

    def remote_compile(self, sig):
        """Compile function and signatures to machine code in remote JIT server.
        """
        # compute LLVM IR module for the given signature
        ir = self.ir_cache.get(sig)
        if ir is not None:
            return
        ir = self.get_IR([sig])
        mangled_signatures = ';'.join([s.mangle() for s in [sig]])
        response = self.client(remotejit=dict(
            compile=(self.func.__name__, mangled_signatures, ir)))
        assert response['remotejit']['compile']
        self.ir_cache[sig] = ir

    def remote_call(self, sig, arguments):
        fullname = self.func.__name__ + sig.mangle()
        response = self.client(remotejit=dict(call=(fullname, arguments)))
        return response['remotejit']['call']

    def __call__(self, *arguments):
        """Return the result of a remote JIT compiled function call.
        """
        atypes = tuple(map(Type.fromvalue, arguments))
        match_sig = None
        match_penalty = None
        for sig in self._signatures:
            penalty = sig.match(atypes)
            if penalty is not None:
                if match_sig is None or penalty < match_penalty:
                    match_sig = sig
                    match_penalty = penalty
        if match_sig is None:
            raise TypeError(
                'could not find matching function to given argument types:'
                ' `%s`' % (', '.join(map(str, atypes))))
        self.remote_compile(match_sig)
        return self.remote_call(match_sig, arguments)
