
import os
from .caller import Caller
from .remotejit import RemoteJIT
from .thrift.utils import resolve_includes
from pymapd.cursor import make_row_results_set
from pymapd._parsers import _extract_description  # , _bind_parameters


class CallerMapD(Caller):
    """
    """
    def __init__(self, *args, **kwargs):
        Caller.__init__(self, *args, **kwargs)
        self.register()

    def thrift_call(self, name, *args):
        return self.remotejit.thrift_call(name, *args)

    def get_MapD_version(self):
        return self.remotejit.version

    _session_id = None
    @property
    def session_id(self):
        return self.remotejit.session_id

    def sql_execute(self, query):
        return self.remotejit.sql_execute(query)

    def register(self):
        device_target_map = self.thrift_call('get_device_target_map')

        signatures = self._signatures
        for i, sig in enumerate(signatures):
            if i == 0:
                sig.set_mangling('')
            else:
                sig.set_mangling('__%s' % (i))
        ir = self.compile_to_IR(signatures, targets=device_target_map.values())

        name = self.func.__name__
        ast_signatures = '\n'.join(["%s%s '%s'" % (name, s.mangling, s)
                                    for i, s in enumerate(signatures)])

        device_ir_map = {}
        for device, target in device_target_map.items():
            device_ir_map[device] = ir[target]
        return self.thrift_call('register_runtime_udf', self.session_id,
                                ast_signatures, device_ir_map)


class RemoteMapD(RemoteJIT):

    """Usage:

      mapd = RemoteMapD(host=..., port=...)

      @mapd
      def add(a, b):
          return a + b

      add.register()

      Use pymapd, for instance, to make a SQL query `select add(c1,
      c2) from table`

    """

    caller_cls = CallerMapD
    multiplexed = False
    mangle_prefix = ''

    def __init__(self,
                 user='mapd',
                 password='HyperInteractive',
                 host='127.0.0.1',
                 port=6274,
                 dbname='mapd',
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
        columnar = True
        result = self.thrift_call(
            'sql_execute', self.session_id, query, columnar, "", -1, -1)
        descr = _extract_description(result.row_set.row_desc)
        return descr, make_row_results_set(result)
