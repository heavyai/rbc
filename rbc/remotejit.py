# Author: Pearu Peterson
# Created: February 2019

__all__ = ['RemoteJIT', 'Signature', 'Caller']

import os
import inspect
import warnings
from . import irtools
from .typesystem import Type, get_signature
from .thrift import Server, Dispatcher, dispatchermethod, Data, Client
from .utils import get_local_ip
from .targetinfo import TargetInfo


def isfunctionlike(obj):
    """Return True if object is function alike.
    """
    if obj is None or isinstance(obj, (Signature, list, tuple, str, Caller)):
        return False
    return True


def extract_templates(options):
    """Extract templates mapping data from options dictionary.

    If options does not contain "templates", it will be constructed
    from all unknown options that have list values. Otherwise, the
    corresponding value is returned with no further processing of
    options content.

    Parameters
    ----------
    options : dict

    Returns
    -------
    options : dict
      A copy of input without templates mapping data.
    templates : dict
      Templates mapping which is a collections of pairs of template
      name and a list of concrete types. Template name cannot
      correspond to a concrete type.

    """
    known_options = ['devices', 'local']
    new_options = {}
    templates = options.get('templates')
    if templates is not None:
        new_options.update(options)
        del new_options['templates']
    else:
        templates = {}
        for k, v in options.items():
            if (isinstance(k, str) and isinstance(v, list) and k not in known_options):
                templates[k] = v
            else:
                new_options[k] = v
    return new_options, templates


class Signature(object):
    """Signature decorator for Python functions.

    A Signature decorator may contain many signature objects
    representing the prototypes of functions.

    Signature decorators are re-usable and composeable. For example:

    .. highlight:: python
    .. code-block:: python

        rjit = RemoteJIT(host='localhost' port=6274)

        # remotebinaryfunc is Signature instance
        remotebinaryfunc = rjit('int32(int32, int32)',
                                'float32(float32, float32)', ...)

        # add will be Caller instance
        @remotebinaryfunc
        def add(a, b):
            return a + b

        # sub will be Caller instance
        @remotebinaryfunc
        def sub(a, b):
            return a - b

        add(1, 2) # returns 3
        sub(1.0, 2.0) # returns -1.0
    """

    def __init__(self, remotejit):
        assert isinstance(remotejit, RemoteJIT), type(remotejit)
        self.remotejit = remotejit
        self.signatures = []
        self.signature_devices = {}
        self.signature_templates = {}

    @property
    def debug(self):
        return self.remotejit.debug

    @property
    def local(self):
        sig = Signature(self.remotejit.local)
        sig.signatures.extend(self.signatures)
        assert not self.signature_devices
        assert not self.signature_templates
        return sig

    def __str__(self):
        lst = ["'%s'" % (s,) for s in self.signatures]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(lst))

    def __call__(self, obj, **options):
        """Decorate signatures or a function.

        Parameters
        ----------
        obj : {str, Signature, function, ...}
          Specify object that represents a function type.

        Keyword parameters
        ------------------
        devices : list
          Specify device names for the given set of signatures.
        templates : dict
          Specify template types mapping.

        Returns
        -------
        result : {Signature, Caller}
          If obj is a function, return Caller. Otherwise return self
          that is extended with new signatures from obj.

        Note
        ----
        The validity of the input argument is not checked here.  This
        is because the bit-size of certain C types (e.g. size_t, long,
        etc) depend on the target device which information will be
        available at the compile stage. The target dependent
        signatures can be retrieved using
        `signature.get_signatures()`.
        """
        if obj is None:
            return self
        options, templates = extract_templates(options)
        devices = options.get('devices')
        if isinstance(obj, Signature):
            self.signatures.extend(obj.signatures)
            self.signature_devices.update(obj.signature_devices)
            self.remotejit.discard_last_compile()
            if devices is not None:
                for s in obj.signatures:
                    self.signature_devices[s] = devices
            assert not templates
            for s in obj.signatures:
                t = obj.signature_templates.get(s)
                if t is not None:
                    self.signature_templates[s] = t
            return self
        if isinstance(obj, Caller):
            # return new Caller with extended signatures set
            assert obj.remotejit is self.remotejit
            final = Signature(self.remotejit)
            final(self)  # copies the signatures from self to final
            final(obj.signature)  # copies the signatures from obj to final
            assert devices is None
            assert not templates
            return Caller(obj.func, final)
        if isfunctionlike(obj):
            final = Signature(self.remotejit)
            final(self)  # copies the signatures from self to final
            assert devices is None
            assert not templates
            return Caller(obj, final)
        self.signatures.append(obj)
        self.remotejit.discard_last_compile()
        if devices is not None:
            self.signature_devices[obj] = devices
        if templates:
            self.signature_templates[obj] = templates
        return self

    def best_match(self, func, atypes: tuple) -> Type:
        """Return function type from signatures that matches best with given
        argument types.

        If no match is found, raise TypeError.

        Parameters
        ----------
        atypes : Type-tuple
          Specify a tuple of argument types.

        Returns
        -------
        ftype : Type
          Function type that arguments match best with given argument
          types.
        """
        ftype = None
        match_penalty = None
        available_types = self.normalized(func).signatures
        for typ in available_types:
            penalty = typ.match(atypes)
            if penalty is not None:
                if ftype is None or penalty < match_penalty:
                    ftype = typ
                    match_penalty = penalty
        if ftype is None:
            satypes = ', '.join(map(str, atypes))
            available = '; '.join(map(str, available_types))
            raise TypeError(
                f'found no matching function type to given argument types'
                f' `{satypes}`. Available function types: {available}')
        return ftype

    def normalized(self, func=None):
        """Return a copy of Signature object where all signatures are
        normalized to Type instances using the current target device
        information.

        Parameters
        ----------
        func : {None, callable}
          Python function that annotations are attached to signature.

        Returns
        -------
        signature : Signature
        """
        signature = Signature(self.remotejit)
        fsig = Type.fromcallable(func) if func is not None else None
        nargs = fsig.arity if func is not None else None
        target_info = TargetInfo()
        for sig in self.signatures:
            devices = self.signature_devices.get(sig)
            if not target_info.check_enabled(devices):
                if self.debug:
                    print(f'{type(self).__name__}.normalized: skipping {sig} as'
                          f' not supported by devices: {devices}')
                continue
            templates = self.signature_templates.get(sig, {})
            sig = Type.fromobject(sig)
            if not sig.is_complete:
                warnings.warn(f'Incomplete signature {sig} will be ignored')
                continue
            if not sig.is_function:
                raise ValueError(
                    'expected signature representing function type,'
                    f' got `{sig}`')
            if nargs is None:
                nargs = sig.arity
            elif sig.arity != nargs:
                raise ValueError(f'signature `{sig}` must have arity {nargs}'
                                 f' but got {len(sig[1])}')
            if fsig is not None:
                sig.inherit_annotations(fsig)

            if not sig.is_concrete:
                for csig in sig.apply_templates(templates):
                    assert isinstance(csig, Type), (sig, csig, type(csig))
                    if csig not in signature.signatures:
                        signature.signatures.append(csig)
            else:
                if sig not in signature.signatures:
                    signature.signatures.append(sig)
        if fsig is not None and fsig.is_complete:
            if fsig not in signature.signatures:
                signature.signatures.append(fsig)
        return signature


class Caller(object):
    """Remote JIT caller, holds the decorated function that can be
    executed remotely.
    """

    def __init__(self, func, signature: Signature):
        """Construct remote JIT caller instance.

        Parameters
        ----------
        func : callable
          Specify a Python function that is used as a template to
          remotely JIT compiled functions.
        signature : Signature
          Specify a collection of signatures.
        local : bool
          When True, local process will be interpreted as
          remote. Useful for debugging.
        """
        self.remotejit = signature.remotejit
        self.signature = signature
        func = self.remotejit.preprocess_callable(func)
        self.func = func
        self.nargs = len(get_signature(func).parameters)

        # Attributes used in RBC user-interface
        self._is_compiled = set()  # items are (fname, ftype)
        self._client = None

        self.remotejit.add_caller(self)

    @property
    def local(self):
        """Return Caller instance that executes function calls on the local
        host. Useful for debugging.
        """
        return Caller(self.func, self.signature.local)

    def __repr__(self):
        return '%s(%s, %s, local=%s)' % (type(self).__name__, self.func,
                                         self.signature, self.local)

    def __str__(self):
        return self.describe()

    def describe(self):
        """Return LLVM IRs of all target devices.
        """
        lst = ['']
        fid = 0
        for device, target_info in self.remotejit.targets.items():
            with target_info:
                lst.append(f'{device:-^80}')
                signatures = self.get_signatures()
                signatures_map = {}
                for sig in signatures:
                    fid += 1
                    signatures_map[fid] = sig
                llvm_module, succesful_fids = irtools.compile_to_LLVM(
                    [(self.func, signatures_map)],
                    target_info,
                    debug=self.remotejit.debug)
                lst.append(str(llvm_module))
        lst.append(f'{"":-^80}')
        return '\n'.join(lst)

    def get_signatures(self):
        """Return a list of normalized signatures for given target device.
        """
        return self.signature.normalized(self.func).signatures

    # RBC user-interface

    def __call__(self, *arguments, **options):
        """Return the result of a remote JIT compiled function call.
        """
        device = options.get('device')
        targets = self.remotejit.targets
        if device is None:
            if len(targets) > 1:
                raise TypeError(
                    f'specifying device is required when target has more than'
                    f' one device. Available devices: {", ".join(targets)}')
            device = tuple(targets)[0]
        target_info = targets[device]
        with target_info:
            atypes = tuple(map(Type.fromvalue, arguments))
            ftype = self.signature.best_match(self.func, atypes)
            key = self.func.__name__, ftype
            if key not in self._is_compiled:
                self.remotejit.remote_compile(self.func, ftype, target_info)
                self._is_compiled.add(key)
            return self.remotejit.remote_call(self.func, ftype, arguments)


class RemoteJIT(object):
    """RemoteJIT is a decorator generator for user functions to be
    remotely JIT compiled.

    To use, define

    .. highlight:: python
    .. code-block:: python

        rjit = RemoteJIT(host='localhost', port=6274)

        @rjit
        def foo(a: int, b: int) -> int:
            return a + b

        @rjit('double(double, double)',
              'int64(int64, int64)')
        def bar(a, b):
            return a + b

        # Finally, call
        c = foo(1, 2) # c = 3
        b = bar(7.0, 1.0) # b = 8.0

    The sum will be evaluated in the remote host.
    """

    multiplexed = True

    thrift_content = None

    def __init__(self, host='localhost', port=11530,
                 local=False, debug=False):
        """Construct remote JIT function decorator.

        The decorator is re-usable for different functions.

        Parameters
        ----------
        host : str
          Specify the host name of IP of JIT server
        port : {int, str}
          Specify the service port of the JIT server
        local : bool
          When True, use local client. Useful for debugging.
        debug : bool
          When True, output debug messages.
        """
        if host == 'localhost':
            host = get_local_ip()

        self.debug = debug
        self.host = host
        self.port = int(port)
        self.server_process = None

        # A collection of Caller instances. Each represents a function
        # that have many argument type dependent implementations.
        self._callers = []
        self._last_compile = None
        self._targets = None

        if local:
            self._client = LocalClient()
        else:
            self._client = None

    @property
    def local(self):
        localjit = type(self)(local=True)
        localjit._callers.extend(self._callers)
        return localjit

    def add_caller(self, caller):
        self._callers.append(caller)
        self.discard_last_compile()

    def get_callers(self):
        return self._callers

    def reset(self):
        """Drop all callers definitions and compilation results.
        """
        self._callers.clear()
        self.discard_last_compile()

    @property
    def have_last_compile(self):
        """Check if compile data exists.

        See `set_last_compile` method for more information.
        """
        return self._last_compile is not None

    def discard_last_compile(self):
        """Discard compile data.

        See `set_last_compile` method for more information.
        """
        self._last_compile = None

    def set_last_compile(self, compile_data):
        """Save compile data.

        The caller is responsible for discarding previous compiler
        data by calling `discard_last_compile` method.

        Parameters
        ----------
        compile_data : object
          Compile data can be any Python object. When None, it is
          interpreted as no compile data is available.

        Usage
        -----

        The have/discard/set_last_compile methods provide a way to
        avoid unnecessary compilations when the remote server supports
        registration of compiled functions. The corresponding
        `register` method is expected to use the following pattern:

        ```
          def register(self):
              if self.have_last_compile:
                  return
              <compile defined functions>
              self.set_last_compile(<compilation results>)
        ```

        The `discard_last_compile()` method is called when the compile
        data becomes obsolete or needs to be discarded. For instance,
        the compile data will be discarded when calling the following
        methods: `reset`, `add_caller`. Note that the `add_caller`
        call is triggered when applying the remotejit decorator to a
        Python function to be compiled.

        """
        assert self._last_compile is None
        self._last_compile = compile_data

    def retrieve_targets(self):
        """Retrieve target device information from remote client.

        Redefine this method if remote client is not native.

        Returns
        -------
        targets : dict
          Map of target device names and informations.
        """
        # TODO: rename thrift API targets to get_device_parameters?
        response = self.client(remotejit=dict(targets=()))
        targets = {}
        for device, data in response['remotejit']['targets'].items():
            targets[device] = TargetInfo.fromjson(data)
        return targets

    @property
    def targets(self):
        """Return device-target_info mapping of the remote server.
        """
        if self._targets is None:
            self._targets = self.retrieve_targets()
        return self._targets

    def __call__(self, *signatures, **options):
        """Define a remote JIT function signatures and template.

        Parameters
        ----------
        signatures : tuple
          Specify signatures of a remote JIT function, or a Python
          function as a template from which the remote JIT function
          will be compiled.

        Keyword parameters
        ------------------
        local : bool
        devices : list
          Specify device names for the given set of signatures.
        templates : dict
          Specify template types mapping.

        Returns
        -------
        sig: {Signature, Caller}
          Signature decorator or Caller

        Notes
        -----
        The signatures can be strings in the following form:

          "<return type>(<argument type 1>, <argument type 2>, ...)"

        or any other object that can be converted to function type,
        see `Type.fromobject` for more information.
        """
        if options.get('local'):
            s = Signature(self.local)
        else:
            s = Signature(self)
        devices = options.get('devices')
        options, templates = extract_templates(options)
        for sig in signatures:
            s = s(sig, devices=devices, templates=templates)
        return s

    def start_server(self, background=False):
        """Start remotejit server from client.
        """
        thrift_file = os.path.join(os.path.dirname(__file__),
                                   'remotejit.thrift')
        print('staring rpc.thrift server: %s' % (thrift_file), end='',
              flush=True)

        if self.debug:
            print(flush=True)
            dispatcher = DebugDispatcherRJIT
        else:
            dispatcher = DispatcherRJIT
        if background:
            ps = Server.run_bg(dispatcher, thrift_file,
                               dict(host=self.host, port=self.port))
            self.server_process = ps
        else:
            Server.run(dispatcher, thrift_file,
                       dict(host=self.host, port=self.port))
            print('... rpc.thrift server stopped', flush=True)

    def stop_server(self):
        """Stop remotejit server from client.
        """
        if self.server_process is not None and self.server_process.is_alive():
            print('... stopping rpc.thrift server')
            self.server_process.terminate()
            self.server_process = None

    @property
    def client(self):
        """Return remote host connection as Client instance.
        """
        if self._client is None:
            self._client = Client(
                host=self.host,
                port=self.port,
                multiplexed=self.multiplexed,
                thrift_content=self.thrift_content,
                socket_timeout=60000)
        return self._client

    def remote_compile(self, func, ftype: Type, target_info: TargetInfo):
        """Remote compile function and signatures to machine code.

        The input function `func` is compiled to LLVM IR module, the
        LLVM IR module is sent to remote host where the remote host is
        expected to complete the compilation process.

        Return the corresponding LLVM IR module instance which may be
        useful for debugging.
        """
        llvm_module, succesful_fids = irtools.compile_to_LLVM(
            [(func, {0: ftype})], target_info, debug=self.debug)
        ir = str(llvm_module)
        mangled_signatures = ';'.join([s.mangle() for s in [ftype]])
        response = self.client(remotejit=dict(
            compile=(func.__name__, mangled_signatures, ir)))
        assert response['remotejit']['compile'], response
        return llvm_module

    def remote_call(self, func, ftype: Type, arguments: tuple):
        """Call function remotely on given arguments.

        The input function `func` is called remotely by sending the
        arguments data to remote host where the previously compiled
        function (see `remote_compile` method) is applied to the
        arguments, and the result is returned to local process.
        """
        fullname = func.__name__ + ftype.mangle()
        response = self.client(remotejit=dict(call=(fullname, arguments)))
        return response['remotejit']['call']

    def preprocess_callable(self, func):
        """Preprocess func to be used as a remotejit function definition.

        Parameters
        ----------
        func : callable

        Returns
        -------
        func : callable
          Preprocessed func.
        """
        return func


class DispatcherRJIT(Dispatcher):
    """Implements remotejit service methods.
    """

    debug = False

    def __init__(self, server):
        Dispatcher.__init__(self, server)
        self.compiled_functions = dict()
        self.engines = dict()

    @dispatchermethod
    def targets(self) -> dict:
        """Retrieve target device information.

        Returns
        -------
        info : dict
          Map of target devices and their properties.
        """
        target_info = TargetInfo.host()
        target_info.set('has_numba', True)
        target_info.set('has_cpython', True)
        return dict(cpu=target_info.tojson())

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
            if self.debug:
                print(f'compile({name}, {sig}) -> {hex(addr)}')
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
        if self.debug:
            print(f'call({fullname}, {arguments})')
        ef = self.compiled_functions.get(fullname)
        if ef is None:
            raise RuntimeError('no such compiled function `%s`' % (fullname))
        r = ef[1](*arguments)
        if hasattr(r, 'topython'):
            return r.topython()
        return r


class DebugDispatcherRJIT(DispatcherRJIT):
    """
    Enables debug messages.
    """
    debug = True


class LocalClient(object):
    """Pretender of thrift.Client.

    All calls will be made in a local process. Useful for debbuging.
    """

    def __init__(self):
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
