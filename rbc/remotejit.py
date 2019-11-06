# Author: Pearu Peterson
# Created: February 2019

import os
import inspect
import warnings

from . import irtools
from .caller import Caller
from .typesystem import Type, LocalTargetInfo
from .thrift import Server, Dispatcher, dispatchermethod, Data, Client
from .utils import get_local_ip


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

    multiplexed = True

    thrift_content = None

    def __init__(self, host='localhost', port=11530, **options):
        """Construct remote JIT function decorator.

        The decorator is re-usable for different functions.

        Parameters
        ----------
        host : str
          Specify the host name of IP of JIT server
        port : {int, str}
          Specify the service port of the JIT server
        options : dict
          Specify default options.
        """
        if host == 'localhost':
            host = get_local_ip()

        target_info = options.pop('target_info', None)
        if target_info is None:
            target_info = LocalTargetInfo(strict=True)
        self.target_info = target_info

        self.debug = options.pop('debug', False)
        self.host = host
        self.port = int(port)
        self.options = options
        self.server_process = None
        self.callers = []
        self._last_ir_map = None

    def reset(self):
        self.callers = []
        self.discard_last_compile()

    @property
    def have_last_compile(self):
        return self._last_ir_map is not None

    def discard_last_compile(self):
        self._last_ir_map = None

    def append(self, caller: Caller):
        """Add caller to the list of callers containing functions and their
        signatures to be compiled.
        """
        self.callers.append(caller)
        self.discard_last_compile()

    def compile_to_LLVM(self, targets):
        if self._last_ir_map is None:
            functions_and_signatures = [(caller.func, caller._signatures)
                                        for caller in self.callers]
            ir_map = {}
            for target in targets:
                ir_map[target] = irtools.compile_to_LLVM(
                    functions_and_signatures, target,
                    debug=self.debug, **self.options)
            self._last_ir_map = ir_map
        return self._last_ir_map

    def __call__(self, *signatures, **options):
        """Define a remote JIT function signatures and content.

        Parameters
        ----------
        signatures : tuple
          Specify signatures of a remote JIT function, or a Python
          function as a content from which the remote JIT function
          will be compiled.
        options : dict
          Specify options that overwide the default options.

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
        s = Signature(self, options=options)
        func = None
        for sig in signatures:
            if inspect.isfunction(sig):
                assert func is None, repr(func)
                func = sig
            elif isinstance(sig, Signature):
                s = s(sig)
            else:
                s = s(Type.fromobject(sig, self.target_info))
        # s is Signature instance
        if func is not None:
            # s becomes Caller instance
            s = s(func, **(options or self.options))
        return s

    def start_server(self, background=False):
        thrift_file = os.path.join(os.path.dirname(__file__),
                                   'remotejit.thrift')
        print('staring rpc.thrift server: %s' % (thrift_file), end='')
        if background:
            ps = Server.run_bg(DispatcherRJIT, thrift_file,
                               dict(host=self.host, port=self.port))
            self.server_process = ps
        else:
            Server.run(DispatcherRJIT, thrift_file,
                       dict(host=self.host, port=self.port))
            print('... rpc.thrift server stopped')

    def stop_server(self):
        if self.server_process is not None and self.server_process.is_alive():
            print('... stopping rpc.thrift server')
            self.server_process.terminate()
            self.server_process = None

    def make_client(self):
        return Client(
            host=self.host,
            port=self.port,
            multiplexed=self.multiplexed,
            thrift_content=self.thrift_content,
            socket_timeout=10000)


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

    def __init__(self, remotejit, options=None):
        self.remotejit = remotejit   # RemoteJIT
        self.signatures = []
        if options is None:
            options = remotejit.options
        self.options = options

    def __str__(self):
        lst = ["'%s'" % (s,) for s in self.signatures]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(lst))

    def __call__(self, obj):
        if obj is None:
            return self
        if inspect.isfunction(obj):
            t = Type.fromcallable(obj, target_info=self.remotejit.target_info)
            if t.is_complete:
                self.signatures.append(t)
            signatures = self.signatures
            self.signatures = []  # allow reusing the Signature instance
            for caller in self.remotejit.callers:
                if (caller.func.__name__ == obj.__name__
                    and caller.nargs == len(
                        inspect.signature(obj).parameters)):
                    for sig in signatures:
                        if sig not in caller._signatures:
                            continue
                        f1 = os.path.basename(obj.__code__.co_filename)
                        f2 = os.path.basename(caller.func.__code__.co_filename)
                        n1 = obj.__code__.co_firstlineno
                        n2 = caller.func.__code__.co_firstlineno
                        warnings.warn(
                            '{f1}#{n1} re-implements {f2}#{n2} for `{sig}`'
                            .format(f1=f1, n1=n1, f2=f2, n2=n2, sig=sig))
                        del caller._signatures[caller._signatures.index(sig)]
            return Caller(self.remotejit, signatures, obj,
                          **self.options)  # finalized caller
        elif isinstance(obj, Caller):
            signatures = obj._signatures + self.signatures
            return Caller(self.remotejit, signatures,
                          obj.func, **self.options)
        elif isinstance(obj, type(self)):
            self.signatures.extend(obj.signatures)
            return self
        else:
            self.signatures.append(
                Type.fromobject(obj, target_info=self.remotejit.target_info))
            return self


class DispatcherRJIT(Dispatcher):
    """Implements remotejit service methods.
    """

    def __init__(self, server):
        Dispatcher.__init__(self, server)
        self.compiled_functions = dict()
        self.engines = dict()

    @dispatchermethod
    def compile(self, name: str, signatures: str, ir: str) -> int:
        """JIT compile function.

        Parameters
        ----------
        name : str
          Specify the function name.
        signatures : str
          Specify semi-colon separated list of mangled signatures.
        ir : str
          Specify LLVM IR representation of the function.
        """
        engine = irtools.compile_IR(ir)
        for msig in signatures.split(';'):
            sig = Type.demangle(msig)
            fullname = name + msig
            addr = engine.get_function_address(fullname)
            # storing engine as the owner of function addresses
            self.compiled_functions[fullname] = engine, sig.toctypes()(addr)
        return True

    @dispatchermethod
    def call(self, fullname: str, arguments: tuple) -> Data:
        """Call JIT compiled function

        Parameters
        ----------
        fullname : str
          Specify the full name of the function that is in form
          "<name><mangled signature>"
        arguments : tuple
          Speficy the arguments to the function.
        """
        ef = self.compiled_functions.get(fullname)
        if ef is None:
            raise RuntimeError('no such compiled function `%s`' % (fullname))
        r = ef[1](*arguments)
        if hasattr(r, 'topython'):
            return r.topython()
        return r


class LocalClient(object):
    """Pretender of thrift.Client.

    All calls will be made in a local process. Useful for debbuging.
    """

    def __init__(self, **options):
        self.options = options
        self.dispatcher = DispatcherRJIT(None)

    def __call__(self, **services):
        results = {}
        for service_name, query_dict in services.items():
            results[service_name] = {}
            for mthname, args in query_dict.items():
                mth = getattr(self.dispatcher, mthname)
                mth = inspect.unwrap(mth)
                results[service_name][mthname] = mth(self.dispatcher, *args)
        return results
