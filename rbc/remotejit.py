"""RemoteJIT client/server config functions
"""

__all__ = ['RemoteJIT', 'Signature', 'Caller']

import os
import inspect
import warnings
import ctypes
from collections import defaultdict
from . import irtools
from .errors import UnsupportedError
from .typesystem import Type, get_signature
from .thrift import Server, Dispatcher, dispatchermethod, Data, Client
from .utils import get_local_ip, UNSPECIFIED
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


class Signature:
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

    def __repr__(self):
        return f'{type(self).__name__}({str(self)})'

    def __str__(self):
        lst = []
        for t in self.signatures:
            s = str(t)
            for k, types in self.signature_templates.get(t, {}).items():
                s += f', {k}={"|".join(map(str, types))}'
            devices = self.signature_devices.get(t, [])
            if devices:
                s += f', device={"|".join(devices)}'
            lst.append(repr(s))
        return f'{"; ".join(lst)}'

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
            sig = self.remotejit.caller_signature(typ)
            penalty = sig.match(atypes)
            if penalty is not None:
                if ftype is None or penalty < match_penalty:
                    ftype = typ
                    match_penalty = penalty
        return ftype, match_penalty

    def add(self, sig):
        if sig not in self.signatures:
            self.signatures.append(sig)

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
                    csig = self.remotejit.normalize_function_type(csig)
                    signature.add(csig)
            else:
                sig = self.remotejit.normalize_function_type(sig)
                signature.add(sig)
        if fsig is not None and fsig.is_complete:
            fsig = self.remotejit.normalize_function_type(fsig)
            signature.add(fsig)
        return signature


class Caller:
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
        if not self.remotejit.supports_local_caller:
            msg = (
                "Cannot create a local `Caller` when using "
                f"{type(self.remotejit).__name__}."
            )
            raise UnsupportedError(msg)

        return Caller(self.func, self.signature.local)

    def __repr__(self):
        return f"{type(self).__name__}({self.func}, {self.signature!r})"

    def __str__(self):
        return f"{self.func.__name__}[{self.signature}]"

    def describe(self):
        """Return LLVM IRs of all target devices.
        """
        # To-Do: Move the pipeline to outside heavydb_backend
        from rbc.heavydb.pipeline import HeavyDBCompilerPipeline

        lst = ['']
        fid = 0
        for device, target_info in self.remotejit.targets.items():
            with Type.alias(**self.remotejit.typesystem_aliases):
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
                        pipeline_class=HeavyDBCompilerPipeline,
                        debug=self.remotejit.debug)
                    lst.append(str(llvm_module))
        lst.append(f'{"":-^80}')
        return '\n'.join(lst)

    def get_signatures(self):
        """Return a list of normalized signatures for given target device.
        """
        return self.signature.normalized(self.func).signatures

    # RBC user-interface

    def __call__(self, *arguments, device=UNSPECIFIED, hold=UNSPECIFIED):
        """Return the result of a remote JIT compiled function call.
        """
        caller = self.remotejit.get_caller(self.func.__name__)
        return caller(*arguments, device=device, hold=hold)


class RemoteDispatcher:
    """A collection of Caller instances holding functions with a common name.
    """
    def __init__(self, name, callers):
        self.name = name
        assert callers  # at least one caller must be specified
        self.remotejit = callers[0].remotejit
        self.callers = callers

    def __repr__(self):
        lst = [str(caller.signature) for caller in self.callers]
        return f'{type(self).__name__}({self.name!r}, [{", ".join(lst)}])'

    def __str__(self):
        lst = [str(caller.signature) for caller in self.callers]
        return f'{self.name}[{", ".join(lst)}]'

    def __call__(self, *arguments, device=UNSPECIFIED, hold=UNSPECIFIED):
        """Perform remote call with given arguments.

        If `hold` is True, return an object that encapsulates the
        remote call to postpone the remote execution.
        """
        if hold is UNSPECIFIED:
            hold = self.remotejit.default_remote_call_hold

        penalty_device_caller_ftype = []
        atypes = None
        for device_, target_info in self.remotejit.targets.items():
            if device is not UNSPECIFIED and device != device_:
                continue
            with target_info:
                atypes = self.remotejit.get_types(*arguments)
                for caller_id, caller in enumerate(self.callers):
                    with Type.alias(**self.remotejit.typesystem_aliases):
                        ftype, penalty = caller.signature.best_match(caller.func, atypes)
                        if ftype is None:
                            continue
                        penalty_device_caller_ftype.append((penalty, device_, caller_id, ftype))
        if atypes is None:
            raise ValueError(f'no target info found for given device {device}')

        penalty_device_caller_ftype.sort()

        if not penalty_device_caller_ftype:
            available_types_devices = defaultdict(set)
            for device_, target_info in self.remotejit.targets.items():
                if device is not UNSPECIFIED and device != device_:
                    continue
                with target_info:
                    for caller in self.callers:
                        with Type.alias(**self.remotejit.typesystem_aliases):
                            for t in caller.signature.normalized(caller.func).signatures:
                                available_types_devices[t].add(device_)
            lines = self.remotejit._format_available_function_types(available_types_devices)
            available = '\n    ' + '\n    '.join(lines)
            satypes = ', '.join(map(str, atypes))
            raise TypeError(
                f'found no matching function signature to given argument types:'
                f'\n    ({satypes}) -> ...\n  available function signatures:{available}')

        _, device, caller_id, ftype = penalty_device_caller_ftype[0]
        target_info = self.remotejit.targets[device]
        caller = self.callers[caller_id]
        r = self.remotejit.remote_call_capsule_cls(caller, target_info, ftype, arguments)

        return r if hold else r.execute()


class RemoteCallCapsule:
    """Encapsulates remote call execution.
    """

    use_execute_cache = False

    def __init__(self, caller, target_info, ftype, arguments):
        self.caller = caller
        self.target_info = target_info
        self.ftype = ftype
        self.arguments = arguments
        self._execute_cache = UNSPECIFIED

    @property
    def __typesystem_type__(self):
        """The typesystem Type instance of the return value of the remote
        call.
        """
        return self.caller.remotejit.caller_signature(self.ftype)[0]

    def __repr__(self):
        return (f'{type(self).__name__}({self.caller!r}, {self.target_info},'
                f' {self.ftype}, {self.arguments})')

    def __str__(self):
        return f'{self.execute(hold=True)}'

    def execute(self, hold=False):
        """Trigger the remote call execution.

        When `hold` is True, return an object that represents the
        remote call.
        """
        if not hold:
            if self.use_execute_cache and self._execute_cache is not UNSPECIFIED:
                return self._execute_cache

            key = self.caller.func.__name__, self.ftype
            if key not in self.caller._is_compiled:
                self.caller.remotejit.remote_compile(
                    self.caller.func, self.ftype, self.target_info)
                self.caller._is_compiled.add(key)
        result = self.caller.remotejit.remote_call(self.caller.func, self.ftype,
                                                   self.arguments, hold=hold)
        if not hold and self.use_execute_cache:
            self._execute_cache = result
        return result


class RemoteJIT:
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

    typesystem_aliases = dict()

    remote_call_capsule_cls = RemoteCallCapsule

    # Some callers cannot be called locally
    supports_local_caller = True

    # Should calling RemoteDispatcher hold the execution:
    default_remote_call_hold = False

    def __init__(self, host='localhost', port=11532,
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

        if int(os.environ.get('RBC_DEBUG', False)):
            self.debug = True
        else:
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
            self._client = LocalClient(debug=debug)
        else:
            self._client = None

    def __repr__(self):
        return f'{type(self).__name__}(host={self.host!r}, port={self.port})'

    @property
    def local(self):
        localjit = type(self)(local=True, debug=self.debug)
        localjit._callers.extend(self._callers)
        return localjit

    def add_caller(self, caller):
        name = caller.func.__name__
        for c in self._callers:
            if c.name == name:
                c.callers.append(caller)
                break
        else:
            self._callers.append(RemoteDispatcher(name, [caller]))
        self.discard_last_compile()

    def get_callers(self):
        callers = []
        for c in self._callers:
            callers.extend(c.callers)
        return callers

    def get_caller(self, name):
        for c in self._callers:
            if c.name == name:
                return c
        return

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


        Notes
        -----

        The have/discard/set_last_compile methods provide a way to
        avoid unnecessary compilations when the remote server supports
        registration of compiled functions. The corresponding
        `register` method is expected to use the following pattern:


        .. code-block:: python

           def register(self):
               if self.have_last_compile:
                   return
               <compile defined functions>
               self.set_last_compile(<compilation results>)


        The `discard_last_compile()` method is called when the compile
        data becomes obsolete or needs to be discarded. For instance,
        the compile data will be discarded when calling the following
        methods: `reset`, `add_caller`. Note that the `add_caller`
        call is triggered when applying the remotejit decorator to a
        Python function to be compiled.

        """
        assert self._last_compile is None
        self._last_compile = compile_data

    def get_pending_names(self):
        """Return the names of functions that have not been registered to the
        remote server.
        """
        names = set()
        if not self.have_last_compile:
            for caller in reversed(self.get_callers()):
                names.add(caller.func.__name__)
        return names

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

    def __call__(self, *signatures, devices=None, local=False, **templates):
        """Define a remote JIT function signatures and template.

        Parameters
        ----------
        signatures : str or object
          Specify signatures of a remote JIT function, or a Python
          function as a template from which the remote JIT function
          will be compiled.

        Keyword parameters
        ------------------
        local : bool
        devices : list
          Specify device names for the given set of signatures. Possible
          values are 'cpu', 'gpu'.
        templates : dict(str, list(str))
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
        if local:
            s = Signature(self.local)
        else:
            s = Signature(self)
        options = dict(
            local=local,
            devices=devices,
            templates=templates.get('templates') or templates
        )
        if devices is not None and not {'cpu', 'gpu'}.issuperset(devices):
            raise ValueError("'devices' can only be a list with possible "
                             f"values 'cpu', 'gpu' but got {devices}")

        _, templates = extract_templates(options)
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
                               dict(host=self.host, port=self.port,
                                    debug=self.debug))
            self.server_process = ps
        else:
            Server.run(dispatcher, thrift_file,
                       dict(host=self.host, port=self.port,
                            debug=self.debug))
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
        # To-Do: Move the pipeline to outside heavydb_backend
        from rbc.heavydb import HeavyDBCompilerPipeline

        if self.debug:
            print(f'remote_compile({func}, {ftype})')
        with target_info:
            llvm_module, succesful_fids = irtools.compile_to_LLVM(
                [(func, {0: ftype})],
                target_info,
                pipeline_class=HeavyDBCompilerPipeline,
                debug=self.debug)
        ir = str(llvm_module)
        mangled_signatures = ';'.join([s.mangle() for s in [ftype]])
        response = self.client(remotejit=dict(
            compile=(func.__name__, mangled_signatures, ir)))
        assert response['remotejit']['compile'], response
        return llvm_module

    def remote_call(self, func, ftype: Type, arguments: tuple, hold=False):
        """Call function remotely on given arguments.

        The input function `func` is called remotely by sending the
        arguments data to remote host where the previously compiled
        function (see `remote_compile` method) is applied to the
        arguments, and the result is returned to local process.

        If `hold` is True then return an object that specifies remote
        call but does not execute it. The type of return object is
        custom to particular RemoteJIT specialization.
        """
        if self.debug:
            print(f'remote_call({func}, {ftype}, {arguments})')
        fullname = func.__name__ + ftype.mangle()
        call = dict(call=(fullname, arguments))
        if hold:
            return call
        response = self.client(remotejit=call)
        return response['remotejit']['call']

    def python(self, statement):
        """Execute Python statement remotely.
        """
        response = self.client(remotejit=dict(python=(statement,)))
        return response['remotejit']['python']

    def normalize_function_type(self, ftype: Type):
        """Apply RemoteJIT specific hooks to normalized function Type.

        Parameters
        ----------
        ftype: Type
          typesystem type of a function

        Returns
        -------
        ftype: Type
          typesystem type of a function with normalization hools applied
        """
        assert ftype.is_function, ftype
        return ftype

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

    def caller_signature(self, signature: Type):
        """Return signature of a caller.

        Parameters
        ----------
        signature: Type
          Signature of function implementation

        Returns
        -------
        signature: Type
          Signature of function caller
        """
        return signature

    def get_types(self, *values):
        """Convert values to the corresponding typesystem types.
        """
        return tuple(map(Type.fromvalue, values))

    def format_type(self, typ: Type):
        """Convert typesystem type to formatted string.
        """
        return str(typ)

    def _format_available_function_types(self, available_types_devices):
        all_devices = set()
        list(map(all_devices.update, available_types_devices.values()))
        lines = []
        for typ, devices in available_types_devices.items():
            sig = self.caller_signature(typ)
            d = '  ' + '|'.join(devices) + ' only' if len(devices) != len(all_devices) else ''
            s = self.format_type(sig)
            t = self.format_type(typ)
            if sig == typ:
                lines.append(f'{s}{d}')
            else:
                lines.append(f'{s}{d}\n    - {t}')
        return lines


class DispatcherRJIT(Dispatcher):
    """Implements remotejit service methods.
    """

    def __init__(self, server, debug=False):
        super().__init__(server, debug=debug)
        self.compiled_functions = dict()
        self.engines = dict()
        self.python_globals = dict()
        self.python_locals = dict()

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
            ctypes_sig = sig.toctypes()
            assert sig.is_function
            if sig[0].is_aggregate:
                raise RuntimeError(
                    f'Functions with aggregate return type values are not supported,'
                    f' got function `{name}` with `{sig}` signature')
            fullname = name + msig
            addr = engine.get_function_address(fullname)
            if self.debug:
                print(f'compile({name}, {sig}) -> {hex(addr)}')
            # storing engine as the owner of function addresses
            if addr:
                self.compiled_functions[fullname] = engine, ctypes_sig(addr), sig, ctypes_sig
            else:
                warnings.warn('No compilation result for {name}|{sig=}')
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
          Specify the arguments to the function.
        """
        if self.debug:
            print(f'call({fullname}, {arguments})')
        ef = self.compiled_functions.get(fullname)
        if ef is None:
            raise RuntimeError(
                f'no such compiled function `{fullname}`. Available functions:\n'
                f'  {"; ".join(list(self.compiled_functions))}\n.')
        sig = ef[2]
        ctypes_sig = ef[3]
        if len(arguments) == 0:
            assert sig.arity == 1 and sig[1][0].is_void, sig
        else:
            assert len(arguments) == sig.arity, (len(arguments), sig.arity)
        ctypes_arguments = []
        for typ, ctypes_typ, value in zip(sig[1], ctypes_sig._argtypes_, arguments):
            if typ.is_custom:
                typ = typ.get_struct_type()
            if typ.is_struct:
                if isinstance(value, tuple):
                    member_values = [t.toctypes()(value[i]) for i, t in enumerate(typ)]
                else:
                    member_values = [t.toctypes()(getattr(value, t.name)) for t in typ]
                ctypes_arguments.extend(member_values)
            elif typ.is_pointer:
                if isinstance(value, ctypes.c_void_p):
                    value = ctypes.cast(value, ctypes_typ)
                else:
                    value = ctypes.cast(value, ctypes_typ)
                ctypes_arguments.append(value)
            else:
                ctypes_arguments.append(value)
        r = ef[1](*ctypes_arguments)
        if sig[0].is_pointer and sig[0][0].is_void and isinstance(r, int):
            r = ctypes.c_void_p(r)
        if self.debug:
            print(f'-> {r}')
        if hasattr(r, 'topython'):
            return r.topython()
        return r

    @dispatchermethod
    def python(self, statement: str) -> int:
        """Execute Python statement.
        """
        if self.debug:
            print(f'python({statement!r})')

        exec(statement, self.python_globals, self.python_locals)
        return True


class DebugDispatcherRJIT(DispatcherRJIT):
    """
    Enables debug messages.
    """
    debug = True


class LocalClient:
    """Pretender of thrift.Client.

    All calls will be made in a local process. Useful for debbuging.
    """

    def __init__(self, debug=False):
        self.dispatcher = DispatcherRJIT(None, debug=debug)

    def __call__(self, **services):
        results = {}
        for service_name, query_dict in services.items():
            results[service_name] = {}
            for mthname, args in query_dict.items():
                mth = getattr(self.dispatcher, mthname)
                mth = inspect.unwrap(mth)
                results[service_name][mthname] = mth(self.dispatcher, *args)
        return results
