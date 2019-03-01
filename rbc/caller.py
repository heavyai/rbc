# Author: Pearu Peterson
# Created: February 2019
import inspect
import warnings

from .typesystem import Type
from . import irtools


class Caller(object):
    """Remote JIT caller
    """

    def __init__(self, server, signatures, func):
        self.server = server
        self._signatures = []
        self.func = func
        self.current_target = 'host'

        sig = inspect.signature(self.func)
        self.nargs = len(sig.parameters)

        # calculate unique id for function content
        src = inspect.getsource(self.func)
        i = src.find('def')
        j = src.rfind('\n', 0, i)
        tab = src[j:i]
        src = src[j:]  # strip decorators
        src = src.replace(tab, '\n')  # shift left
        # TODO: strip annotations
        # TODO: strip line-by-line the minimum whitespace

        for sig in signatures:
            self.add_signature(sig)

        self.engine = None

    def add_signature(self, sig):
        """Update Caller with a new signature
        """
        if not isinstance(sig, Type):
            sig = Type.fromobject(sig)
        nargs = self.nargs
        if not sig.is_function:
            raise ValueError(
                'expected signature with function kind, got `%s`' % (sig))
        if nargs != len(sig[1]) and not (nargs == 0 and sig[1][0].is_void):
            raise ValueError(
                'mismatch of the number of arguments:'
                ' function %s, signature %s (`%s`)'
                % (nargs, len(sig[1]), sig))
        if sig not in self._signatures:
            self.engine = None  # require new compile
            self._signatures.append(sig)
        else:
            warnings.warn('Caller.add_signature:'
                          ' signature `%s` has been already added' % (sig))

    def target(self, target=None):
        """Return current target. When specified, set a new target.
        """
        old_target = self.current_target
        if target is not None:
            self.current_target = target
        return old_target

    def get_IR(self):
        """Return LLVM IR string of compiled function for the current target.
        """
        return irtools.compile_function_to_IR(self.func, self._signatures,
                                              self.current_target, self.server)

    def compile(self):
        """Compile function and signatures to machine code.
        """
        if self.engine is not None:
            return
        ir = self.get_IR()
        self.engine = irtools.compile_IR(ir)

    def __call__(self, *arguments):
        """Return the result of a remote JIT function call.

        Design
        ------
        1. Establish a connection to server and ask if it supports
        jitting the function for a particular target and if the
        function (with local ID) has been compiled in the server. If
        so, the server sends also any required parameters for
        processing the Python function to a IR representation or the
        remote ID of the function. Otherwise, raise an error.

        2. Compile Python function into IR string representation.

        3. Send the IR string together with signature information to
        server where it will be cached. As a response, recieve the
        remote ID of the function.

        4. Send the arguments together with the remote ID to server
        where the IR will be compiled to machine code (unless done
        eariler already), arguments are processed, and remote function
        will be called. The result will be returned in a response.
        """
        self.compile()

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
                'could not find matching function to given argument types')

        addr = self.engine.get_function_address(
            self.func.__name__ + match_sig.mangle())
        cfunc = match_sig.toctypes()(addr)
        return cfunc(*arguments)
