import os
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
                                       'mapd.thrift')
        content = resolve_includes(
            open(thrift_filename).read(),
            [os.path.dirname(thrift_filename)])
        for p in ['completion_hints.', 'common.', 'serialized_result_set.']:
            content = content.replace(p, '')
        self.thrift_content = content
        RemoteJIT.__init__(self, host=host, port=port, **options)

        self.target_info.add_converter(array_type_converter)

    @property
    def version(self):
        return self.thrift_call('get_version')

    _session_id = None
    @property
    def session_id(self):
        if self._session_id is None:
            user = self.user
            pw = self.password
            dbname = self.dbname
            self._session_id = self.thrift_call('connect', user, pw, dbname)
        return self._session_id

    def thrift_call(self, name, *args, **kwargs):
        client = kwargs.get('client')
        if client is None:
            client = self.make_client()
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

    def register(self):
        if self.have_last_compile:
            return

        thrift_client = self.make_client()
        thrift = thrift_client.thrift

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
        }

        functions_map = defaultdict(list)
        for caller in self.callers:
            key = caller.func.__name__, caller.nargs
            functions_map[key].extend(caller._signatures)

        table_functions = []
        ast_signatures = []
        for (name, nargs), signatures in functions_map.items():
            for i, sig in enumerate(signatures):
                if i == 0:
                    sig.set_mangling('')
                else:
                    sig.set_mangling('__%s' % (i))
                if 'table' in sig[0].annotation():  # UDTF
                    sizer = None
                    sizer_index = -1
                    sigArgTypes = []
                    inputArgTypes = []
                    outputArgTypes = []
                    for i, a in enumerate(sig[1]):
                        _sizer = a.annotation().get('sizer')
                        if _sizer is not None:
                            # expect no more than one sizer argument
                            assert sizer_index == -1
                            sizer_index = i + 1
                            sizer = _sizer
                        atype = ext_arguments_map[
                            a.tostring(use_annotation=False)]
                        if i >= 0:
                            if 'output' in a.annotation():
                                outputArgTypes.append(atype)
                            else:
                                if 'input' in a.annotation():
                                    sigArgTypes.append(a.toprototype())
                                elif 'cursor' in a.annotation():
                                    sigArgTypes.append('Cursor')
                                inputArgTypes.append(atype)
                    if sizer is None:
                        sizer = 'kConstant'
                    sizer_type = getattr(thrift.TOutputBufferSizeType, sizer)
                    signature = "%s%s '%s(%s)'" % (
                        name, sig.mangling, sig[0].toprototype(),
                        ', '.join(sigArgTypes))
                    table_functions.append(
                        (name + sig.mangling, signature,
                         sizer_type, sizer_index,
                         inputArgTypes, outputArgTypes))
                else:  # UDF
                    signature = "%s%s '%s'" % (name, sig.mangling, sig.toprototype())
                    ast_signatures.append(signature)

        ast_signatures = '\n'.join(ast_signatures)
        if self.debug:
            print()
            print(' signatures '.center(80, '-'))
            print(ast_signatures)
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

        if table_functions:
            assert len(table_functions) == 1
            return self.thrift_call('register_table_function', self.session_id,
                                    *(table_functions[0] + (device_ir_map,)),
                                    **dict(client=thrift_client))
        return self.thrift_call('register_runtime_udf', self.session_id,
                                ast_signatures, device_ir_map)
