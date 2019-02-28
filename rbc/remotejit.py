# Author: Pearu Peterson
# Created: February 2019

import inspect

from .caller import Caller
from .typesystem import Type


class RemoteJIT(object):
    """RemoteJIT is a decorator generator for user functions to be
    remotely JIT compiled.

    To use, define

      rjit = RemoteJIT(host=..., port=...)

    and then use

      @rjit
      def foo(a: int, b: int) -> int:
        return a + b

    or

      @rjit('double(double, double)',
            'int64(int64, int64)')
      def foo(a, b):
        return a + b

    Finally, call

      c = foo(1, 2)

    where the sum will be evaluated in the remote host.
    """

    def __init__(self, host='localhost', port=11530, **options):
        """Construct remote JIT function decorator.

        The decorator is re-usable for different functions.

        Parameters
        ----------
        host : str
          Specify the host name of IP of JIT server
        port : int
          Specify the service port of the JIT server

        """
        self.host = host
        self.port = port
        self.options = options

        self.functions = dict()  # not used?

    def __call__(self, *signatures):
        """Define a remote JIT function signatures and content.

        Parameters
        ----------
        signatures : tuple
          Specify signatures of a remote JIT function, or a Python
          function as a content from which the remote JIT function
          will be compiled.

        Returns
        -------
        sig or caller : Signature or Caller
          Signature decorator or a Caller instance of the remote JIT
          function.

        Notes
        -----
        The remote JIT function represents a set of remote functions
        with different signatures as well as different targets. The
        target will be defined by the Caller instance (see its
        `target` method).

        The signatures can be strings in the following form:

          "<return type>(<argument type 1>, <argument type 2>, ...)"

        or numba Signature instance or ctypes CFUNCTYPE instance or a
        Python function that uses annotations for arguments and return
        value.
        """
        s = Signature(self)
        func = None
        for sig in signatures:
            if inspect.isfunction(sig):
                assert func is None, repr(func)
                func = sig
            elif isinstance(sig, Signature):
                s = s(sig)
            else:
                s = s(Type.fromobject(sig))
        # s is Signature instance
        if func is not None:
            # s becomes Caller instance
            s = s(func)
        return s


class Signature(object):
    """Signature decorator for Python functions.

    Signature decorators are re-usable. For example:

      rjit = RemoteJIT(host=..., port=...)

      remotebinaryfunc = rjit('int32(int32, int32)',
                              'float32(float32, float32)', ...)

      @remotebinaryfunc
      def add(a, b):
          return a + b

      @remotebinaryfunc
      def sub(a, b):
          return a - b

      add(1, 2) -> 3
      sub(1, 2) -> -1
    """
    def __init__(self, remote):
        self.remote = remote   # RemoteJIT
        self.signatures = []

    def __str__(self):
        lst = ["'%s'" % (s,) for s in self.signatures]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(lst))

    def __call__(self, obj):
        if obj is None:
            return self
        if inspect.isfunction(obj):
            t = Type.fromcallable(obj)
            if t.is_complete:
                self.signatures.append(t)
            signatures = self.signatures
            self.signatures = []  # allow reusing the Signature instance
            return Caller(self.remote, signatures, obj)  # finalized caller
        elif isinstance(obj, Caller):
            signatures = obj._signatures + self.signatures
            return Caller(self.remote, signatures, obj.func)
        elif isinstance(obj, type(self)):
            self.signatures.extend(obj.signatures)
            return self
        else:
            self.signatures.append(Type.fromobject(obj))
            return self
