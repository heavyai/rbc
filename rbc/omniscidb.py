import os
import re
import warnings
import configparser
from collections import defaultdict
from .remotejit import RemoteJIT
from .thrift.utils import resolve_includes
from pymapd.cursor import make_row_results_set
from pymapd._parsers import _extract_description  # , _bind_parameters
from .omnisci_array import array_type_converter
from .targetinfo import TargetInfo
from .irtools import compile_to_LLVM
from .errors import ForbiddenNameError


def is_available(_cache={}):
    """Return version tuple and None if OmnisciDB server is accessible or
    recent enough. Otherwise return None and the reason about
    unavailability.
    """
    if not _cache:
        config = get_client_config()
        omnisci = RemoteOmnisci(**config)
        try:
            version = omnisci.version
        except Exception as msg:
            _cache['reason'] = 'failed to get OmniSci version: %s' % (msg)
        else:
            print(' OmnisciDB version', version)
            if version[:2] >= (4, 6):
                _cache['version'] = version
            else:
                _cache['reason'] = (
                    'expected OmniSci version 4.6 or greater, got %s'
                    % (version,))
    return _cache.get('version', ()), _cache.get('reason')


def get_client_config(**config):
    """Retrieve the omnisci client configuration parameters from a client
    home directory.

    Note that here the client configurations parameters are those that
    are used to configure the client software such as rbc or pymapd.
    This is different from omnisci instance configuration described in
    https://docs.omnisci.com/latest/4_configuration.html that is used
    for configuring the omniscidb server software.

    In Linux clients, the omnisci client configuration is read from:
    :code:`$HOME/.config/omnisci/client.conf`

    In Windows clients, the configuration is read from
    :code:`%UserProfile/.config/omnisci/client.conf` or
    :code:`%AllUsersProfile/.config/omnisci/client.conf`

    When :code:`OMNISCI_CLIENT_CONF` environment variable is defined then
    the configuration is read from the file specified in this
    variable.

    The configuration file must use configuration language similar to
    one used in MS Windows INI files. For omnisci client
    configuration, the file may contain, for instance::

      [user]
      name: <OmniSciDB user name, defaults to admin>
      password: <OmniSciDB user password>

      [server]
      host: <OmniSciDB server host name or IP, defaults to localhost>
      port: <OmniSciDB server port, defaults to 6274>

    Parameters
    ----------
    config : dict
      Specify configuration parameters that override the parameters
      from configuration file.

    Returns
    -------
    config : dict
      A dictionary of `user`, `password`, `host`, `port`, `dbname` and
      other RemoteJIT options.

    """
    _config = dict(user='admin', password='HyperInteractive',
                   host='localhost', port=6274, dbname='omnisci')
    _config.update(**config)
    config = _config

    conf_file = os.environ.get('OMNISCI_CLIENT_CONF', None)
    if conf_file is not None and not os.path.isfile(conf_file):
        print(f'rbc.omnisci.get_client_config:'
              ' OMNISCI_CLIENT_CONF={conf_file!r}'
              ' is not a file, ignoring.')
        conf_file = None
    if conf_file is None:
        conf_file_base = os.path.join('.config', 'omnisci', 'client.conf')
        for prefix_env in ['UserProfile', 'AllUsersProfile', 'HOME']:
            prefix = os.environ.get(prefix_env, None)
            if prefix is not None:
                fn = os.path.join(prefix, conf_file_base)
                if os.path.isfile(fn):
                    conf_file = fn
                    break
    if conf_file is None:
        return config

    conf = configparser.ConfigParser()
    conf.read(conf_file)

    if 'user' in conf:
        user = conf['user']
        if 'name' in user:
            config['user'] = user['name']
        if 'password' in user:
            config['password'] = user['password']

    if 'server' in conf:
        server = conf['server']
        if 'host' in server:
            config['host'] = server['host']
        if 'port' in server:
            config['port'] = int(server['port'])

    if 'rbc' in conf:
        rbc = conf['rbc']
        for k in ['debug', 'use_host_target']:
            if k in rbc:
                config[k] = rbc.getboolean(k)

    return config


class RemoteOmnisci(RemoteJIT):

    """Usage:

    .. highlight:: python
    .. code-block:: python

        omnisci = RemoteOmnisci(host=..., port=...)

        @omnisci('int(int, int)')
        def add(a, b):
            return a + b

        omnisci.register()

    Use pymapd, for instance, to make a SQL query `select add(c1,
    c2) from table`
    """
    converters = [array_type_converter]
    multiplexed = False
    mangle_prefix = ''

    def __init__(self,
                 user='admin',
                 password='HyperInteractive',
                 host='localhost',
                 port=6274,
                 dbname='omnisci',
                 **options):
        self.user = user
        self.password = password
        self.dbname = dbname

        thrift_filename = os.path.join(os.path.dirname(__file__),
                                       'omnisci.thrift')
        content = resolve_includes(
            open(thrift_filename).read(),
            [os.path.dirname(thrift_filename)])
        for p in ['common.', 'extension_functions.']:
            content = content.replace(p, '')
        self.thrift_content = content

        RemoteJIT.__init__(self, host=host, port=port, **options)

        self._version = None
        self._thrift_client = None
        self._session_id = None
        self._targets = None
        self.thrift_typemap = defaultdict(dict)
        self._init_thrift_typemap()

    def _init_thrift_typemap(self):
        """Initialize thrift type map using client thrift configuration.
        """
        typemap = self.thrift_typemap
        for typename, typ in self.thrift_client.thrift.__dict__.items():
            if hasattr(typ, '_NAMES_TO_VALUES'):
                for name, value in typ._NAMES_TO_VALUES.items():
                    typemap[typename][name] = value

    @property
    def version(self):
        if self._version is None:
            version = self.thrift_call('get_version')
            m = re.match(r'(\d+)[.](\d+)[.](\d+)(.*)', version)
            if m is None:
                raise RuntimeError('Could not parse OmniSci version=%r'
                                   % (version))
            major, minor, micro, dev = m.groups()
            self._version = (int(major), int(minor), int(micro), dev)
        return self._version

    @property
    def session_id(self):
        if self._session_id is None:
            user = self.user
            pw = self.password
            dbname = self.dbname
            self._session_id = self.thrift_call('connect', user, pw, dbname)
        return self._session_id

    @property
    def thrift_client(self):
        if self._thrift_client is None:
            self._thrift_client = self.client
        return self._thrift_client

    def thrift_call(self, name, *args, **kwargs):
        client = kwargs.get('client')
        if client is None:
            client = self.thrift_client

        if name == 'register_runtime_udf' and self.version[:2] >= (4, 9):
            warnings.warn('Using register_runtime_udf is deprecated, '
                          'use register_runtime_extension_functions instead.')
            session_id, signatures, device_ir_map = args
            udfs = []
            sig_re = re.compile(r"\A(?P<name>[\w\d_]+)\s+"
                                r"'(?P<rtype>[\w\d_]+)[(](?P<atypes>.*)[)]'\Z")
            thrift = client.thrift
            ext_arguments_map = self._get_ext_arguments_map()
            for signature in signatures.splitlines():
                m = sig_re.match(signature)
                if m is None:
                    raise RuntimeError(
                        'Failed to parse signature %r' % (signature))
                name = m.group('name')
                rtype = ext_arguments_map[m.group('rtype')]
                atypes = [ext_arguments_map[a.strip()]
                          for a in m.group('atypes').split(',')]
                udfs.append(thrift.TUserDefinedFunction(
                    name, atypes, rtype))
            return self.thrift_call('register_runtime_extension_functions',
                                    session_id, udfs, [], device_ir_map)

        if self.debug:
            msg = 'thrift_call %s%s' % (name, args)
            if len(msg) > 200:
                msg = msg[:180] + '...' + msg[-15:]
            print(msg)
        return client(Omnisci={name: args})['Omnisci'][name]

    def sql_execute(self, query):
        self.register()
        columnar = True
        if self.debug:
            print('  %s;' % (query))
        result = self.thrift_call(
            'sql_execute', self.session_id, query, columnar, "", -1, -1)
        descr = _extract_description(result.row_set.row_desc)
        return descr, make_row_results_set(result)

    _ext_arguments_map = None

    def _get_ext_arguments_map(self):
        if self._ext_arguments_map is not None:
            return self._ext_arguments_map
        thrift = self.thrift_client.thrift
        typemap = self.thrift_typemap
        ext_arguments_map = {
            'int8': typemap['TExtArgumentType']['Int8'],
            'int16': typemap['TExtArgumentType']['Int16'],
            'int32': typemap['TExtArgumentType']['Int32'],
            'int64': typemap['TExtArgumentType']['Int64'],
            'float32': typemap['TExtArgumentType']['Float'],
            'float64': typemap['TExtArgumentType']['Double'],
            'int8*': typemap['TExtArgumentType']['PInt8'],
            'int16*': typemap['TExtArgumentType']['PInt16'],
            'int32*': typemap['TExtArgumentType']['PInt32'],
            'int64*': typemap['TExtArgumentType']['PInt64'],
            'float32*': typemap['TExtArgumentType']['PFloat'],
            'float64*': typemap['TExtArgumentType']['PDouble'],
            'bool': typemap['TExtArgumentType']['Bool'],
            'Array<bool>': typemap['TExtArgumentType']['ArrayInt8'],
            'Array<int8_t>': typemap['TExtArgumentType']['ArrayInt8'],
            'Array<int16_t>': typemap['TExtArgumentType']['ArrayInt16'],
            'Array<int32_t>': typemap['TExtArgumentType']['ArrayInt32'],
            'Array<int64_t>': typemap['TExtArgumentType']['ArrayInt64'],
            'Array<float>': typemap['TExtArgumentType']['ArrayFloat'],
            'Array<double>': typemap['TExtArgumentType']['ArrayDouble'],
            'Cursor': typemap['TExtArgumentType']['Cursor'],
            'void': typemap['TExtArgumentType']['Void'],
            'GeoPoint': typemap['TExtArgumentType']['GeoPoint'],
        }
        ext_arguments_map['{bool* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<bool>']
        ext_arguments_map['{int8* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<int8_t>']
        ext_arguments_map['{int16* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<int16_t>']
        ext_arguments_map['{int32* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<int32_t>']
        ext_arguments_map['{int64* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<int64_t>']
        ext_arguments_map['{float32* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<float>']
        ext_arguments_map['{float64* ptr, uint64 sz, bool is_null}*'] \
            = ext_arguments_map['Array<double>']
        values = list(ext_arguments_map.values())
        for v, n in thrift.TExtArgumentType._VALUES_TO_NAMES.items():
            if v not in values:
                warnings.warn('Adding (%r, thrift.TExtArgumentType.%s) '
                              'to ext_arguments_map in %s' % (n, n, __file__))
                ext_arguments_map[n] = v
        self._ext_arguments_map = ext_arguments_map
        return ext_arguments_map

    def retrieve_targets(self):
        device_params = self.thrift_call('get_device_parameters',
                                         self.session_id)
        typemap = self.thrift_typemap
        device_target_map = {}
        messages = []
        for prop, value in device_params.items():
            if prop.endswith('_triple'):
                device = prop.rsplit('_', 1)[0]
                device_target_map[device] = value

            # Update thrift type map from server configuration
            if 'Type.' in prop:
                typ, member = prop.split('.', 1)
                ivalue = int(value)
                # Any warnings below indicate that the client and
                # server thrift configurations are different. Server
                # configuration will win.
                if typ not in typemap:
                    messages.append(f'thrift type map: add new type {typ}')
                elif member not in typemap[typ]:
                    messages.append(
                        f'thrift type map: add new member {typ}.{member}')
                elif ivalue != typemap[typ][member]:
                    messages.append(
                        f'thrift type map: update {typ}.{member}'
                        f' to {ivalue} (was {typemap[typ][member]})')
                else:
                    continue
                typemap[typ][member] = ivalue
        if messages:
            warnings.warn('\n  '.join([''] + messages))

        targets = {}
        for device, target in device_target_map.items():
            target_info = TargetInfo(name=device)
            for prop, value in device_params.items():
                if not prop.startswith(device + '_'):
                    continue
                target_info.set(prop[len(device)+1:], value)
            if device == 'gpu':
                # TODO: remove this hack
                # see https://github.com/numba/numba/issues/4546
                target_info.set('name', 'skylake')
            targets[device] = target_info
        return targets

    @property
    def forbidden_names(self):
        """Return a list of forbidden function names. See
        https://github.com/xnd-project/rbc/issues/32
        """
        if self.version < (5, 2):
            return ['sinh', 'cosh', 'tanh', 'rint', 'trunc', 'expm1',
                    'exp2', 'log2', 'log1p', 'fmod']
        return []

    def register(self):
        if self.have_last_compile:
            return
        thrift = self.thrift_client.thrift

        udfs = []
        udtfs = []
        device_ir_map = {}
        for device, target_info in self.targets.items():
            functions_and_signatures = []
            function_signatures = defaultdict(list)
            for caller in reversed(self.get_callers()):
                ext_arguments_map = self._get_ext_arguments_map()
                signatures = []
                name = caller.func.__name__
                if name in self.forbidden_names:
                    raise ForbiddenNameError(
                        f'\n\nAttempt to define function with name `{name}`.\n'
                        f'As a workaround, add a prefix to the function name '
                        f'or define it with another name:\n\n'
                        f'   def prefix_{name}(x):\n'
                        f'       return np.trunc(x)\n\n'
                        f'For more information, see: '
                        f'https://github.com/xnd-project/rbc/issues/32')
                for sig in caller.get_signatures(target_info):
                    i = len(function_signatures[name])
                    if sig in function_signatures[name]:
                        if self.debug:
                            f2 = os.path.basename(
                                caller.func.__code__.co_filename)
                            n2 = caller.func.__code__.co_firstlineno
                            print(f'{type(self).__name__}.register: ignoring'
                                  f' older definition of `{name}` for `{sig}`'
                                  f' in {f2}#{n2}.')
                        continue
                    function_signatures[name].append(sig)
                    if i == 0 and self.version < (5, 2):
                        sig.set_mangling('')
                    else:
                        sig.set_mangling('__%s' % (i))
                    if 'table' in sig[0].annotation():  # UDTF
                        sizer = None
                        sizer_index = -1
                        inputArgTypes = []
                        outputArgTypes = []
                        sqlArgTypes = []
                        for i, a in enumerate(sig[1]):
                            _sizer = a.annotation().get('sizer')
                            if _sizer is not None:
                                # expect no more than one sizer argument
                                assert sizer_index == -1
                                sizer_index = i + 1
                                sizer = _sizer
                            atype = ext_arguments_map[
                                a.tostring(use_annotation=False)]
                            if 'output' in a.annotation():
                                outputArgTypes.append(atype)
                            else:
                                if 'input' in a.annotation():
                                    sqlArgTypes.append(atype)
                                elif 'cursor' in a.annotation():
                                    sqlArgTypes.append(
                                        ext_arguments_map['Cursor'])
                                inputArgTypes.append(atype)
                        if sizer is None:
                            sizer = 'kConstant'
                        sizer_type = getattr(
                            thrift.TOutputBufferSizeType, sizer)
                        udtfs.append(thrift.TUserDefinedTableFunction(
                            name + sig.mangling,
                            sizer_type, sizer_index,
                            inputArgTypes, outputArgTypes, sqlArgTypes))
                    else:
                        rtype = ext_arguments_map[sig[0].tostring(
                            use_annotation=False)]
                        atypes = [ext_arguments_map[a.tostring(
                            use_annotation=False)] for a in sig[1]]
                        udfs.append(thrift.TUserDefinedFunction(
                            name + sig.mangling, atypes, rtype))
                    signatures.append(sig)
                functions_and_signatures.append((caller.func, signatures))
            llvm_module = compile_to_LLVM(functions_and_signatures,
                                          target_info, self.debug)
            assert llvm_module.triple == target_info.triple
            assert llvm_module.data_layout == target_info.datalayout
            device_ir_map[device] = str(llvm_module)
        return self.thrift_call('register_runtime_extension_functions',
                                self.session_id,
                                udfs, udtfs, device_ir_map)
