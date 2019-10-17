import os
import re
import warnings
from collections import defaultdict
from .remotejit import RemoteJIT
from .thrift.utils import resolve_includes
from pymapd.cursor import make_row_results_set
from pymapd._parsers import _extract_description  # , _bind_parameters
from .omnisci_array import array_type_converter


class RemoteMapD(RemoteJIT):

    """Usage:

      mapd = RemoteMapD(host=..., port=...)

      @mapd('int(int, int)')
      def add(a, b):
          return a + b

      mapd.register()

      Use pymapd, for instance, to make a SQL query `select add(c1,
      c2) from table`

    """

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

        self.target_info.add_converter(array_type_converter)

        self._version = None
        self._session_id = None
        self._thrift_client = None

    @property
    def version(self):
        if self._version is None:
            version = self.thrift_call('get_version')
            m = re.match(r'(\d+)[.](\d+)[.](\d+)(.*)', version)
            if m is None:
                raise RuntimeError('Could not parse OmniSci version=%r'
                                   % (version))
            major, minor, micro, dev = m.groups()
            self._version = int(major), int(minor), int(micro), dev
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
            self._thrift_client = self.make_client()
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
        return client(MapD={name: args})['MapD'][name]

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
        ext_arguments_map = {
            'int8': thrift.TExtArgumentType.Int8,
            'int16': thrift.TExtArgumentType.Int16,
            'int32': thrift.TExtArgumentType.Int32,
            'int64': thrift.TExtArgumentType.Int64,
            'float32': thrift.TExtArgumentType.Float,
            'float64': thrift.TExtArgumentType.Double,
            'int8*': thrift.TExtArgumentType.PInt8,
            'int16*': thrift.TExtArgumentType.PInt16,
            'int32*': thrift.TExtArgumentType.PInt32,
            'int64*': thrift.TExtArgumentType.PInt64,
            'float32*': thrift.TExtArgumentType.PFloat,
            'float64*': thrift.TExtArgumentType.PDouble,
            'bool': thrift.TExtArgumentType.Bool,
            'Array<bool>': thrift.TExtArgumentType.ArrayInt8,
            'Array<int8_t>': thrift.TExtArgumentType.ArrayInt8,
            'Array<int16_t>': thrift.TExtArgumentType.ArrayInt16,
            'Array<int32_t>': thrift.TExtArgumentType.ArrayInt32,
            'Array<int64_t>': thrift.TExtArgumentType.ArrayInt64,
            'Array<float>': thrift.TExtArgumentType.ArrayFloat,
            'Array<double>': thrift.TExtArgumentType.ArrayDouble,
            'Cursor': thrift.TExtArgumentType.Cursor,
            'void': thrift.TExtArgumentType.Void,
            'GeoPoint': thrift.TExtArgumentType.GeoPoint,
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

    def register(self):
        if self.have_last_compile:
            return

        ext_arguments_map = self._get_ext_arguments_map()
        thrift = self.thrift_client.thrift

        udfs = []
        udtfs = []

        functions_map = defaultdict(list)
        for caller in self.callers:
            key = caller.func.__name__, caller.nargs
            functions_map[key].extend(caller._signatures)
            # todo: eliminate dublicated signatures

        for (name, nargs), signatures in functions_map.items():
            for i, sig in enumerate(signatures):
                if i == 0:
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
                                sqlArgTypes.append(ext_arguments_map['Cursor'])
                            inputArgTypes.append(atype)
                    if sizer is None:
                        sizer = 'kConstant'
                    sizer_type = getattr(thrift.TOutputBufferSizeType, sizer)
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

        device_params = self.thrift_call('get_device_parameters',
                                         self.session_id)
        device_target_map = {}
        for prop, value in device_params.items():
            if prop.endswith('_triple'):
                device = prop.rsplit('_', 1)[0]
                device_target_map[device] = value
        ir_map = self.compile_to_LLVM(targets=device_target_map.values())
        device_ir_map = {}
        for device, target in device_target_map.items():
            llvm_module = ir_map[target]

            require_triple = device_params.get(device+'_triple')
            if require_triple is not None:
                if llvm_module.triple != require_triple:
                    raise RuntimeError(
                        'Expected triple `{}` but LLVM contains `{}`'
                        .format(require_triple, llvm_module.triple))

            require_data_layout = device_params.get(device+'_datalayout')
            if require_data_layout is not None:
                if llvm_module.data_layout != require_data_layout:
                    raise RuntimeError(
                        'Expected data layout `{}` but LLVM contains `{}`'
                        .format(require_data_layout, llvm_module.data_layout))

            ir = device_ir_map[device] = str(llvm_module)
            if self.debug:
                print(('IR[%s]' % (device)).center(80, '-'))
                print(ir)
        if self.debug:
            print('=' * 80)

        return self.thrift_call('register_runtime_extension_functions',
                                self.session_id,
                                udfs, udtfs, device_ir_map)
