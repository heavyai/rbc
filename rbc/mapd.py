
import os
from collections import defaultdict
from .remotejit import RemoteJIT
from .thrift.utils import resolve_includes
from pymapd.cursor import make_row_results_set
from pymapd._parsers import _extract_description  # , _bind_parameters


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
                 host='127.0.0.1',
                 port=6274,
                 dbname='omnisci',
                 **options):
        self.user = user
        self.password = password
        self.dbname = dbname

        thrift_filename = os.path.join(os.path.dirname(__file__),
                                       'mapd.thrift')
        self.thrift_content = resolve_includes(
            open(thrift_filename).read(),
            [os.path.dirname(thrift_filename)]).replace(
                'completion_hints.', '')
        RemoteJIT.__init__(self, host=host, port=port, **options)

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

    def thrift_call(self, name, *args):
        return self.make_client()(MapD={name: args})['MapD'][name]

    def sql_execute(self, query):
        self.register()
        columnar = True
        result = self.thrift_call(
            'sql_execute', self.session_id, query, columnar, "", -1, -1)
        descr = _extract_description(result.row_set.row_desc)
        return descr, make_row_results_set(result)

    def register(self):
        if self.have_last_compile:
            return

        functions_map = defaultdict(list)
        for caller in self.callers:
            key = caller.func.__name__, caller.nargs
            functions_map[key].extend(caller._signatures)
        ast_signatures = []
        for (name, nargs), signatures in functions_map.items():
            for i, sig in enumerate(signatures):
                if i == 0:
                    sig.set_mangling('')
                else:
                    sig.set_mangling('__%s' % (i))
                ast_signatures.append("%s%s '%s'" % (name, sig.mangling, sig))
        ast_signatures = '\n'.join(ast_signatures)

        # print(ast_signatures)

        device_params = self.thrift_call('get_device_parameters')
        device_target_map = {}
        for prop, value in device_params.items():
            if prop.endswith('_triple'):
                device = prop.rsplit('_', 1)[0]
                device_target_map[device] = value
        ir_map = self.compile_to_IR(targets=device_target_map.values())
        device_ir_map = {}
        for device, target in device_target_map.items():
            device_ir_map[device] = ir_map[target]
            # print(ir_map[target])

        return self.thrift_call('register_runtime_udf', self.session_id,
                                ast_signatures, device_ir_map)
