"""Client implements API for calling methods defined in multiplex
thrift server. The clients thrift configuration will be downloaded
from the server (the server has to implement a service `info` with a
method `thrift_content`).

"""
# Author: Pearu Peterson
# Created: February 2019

import os
import tempfile
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import thriftpy2 as thr
    import thriftpy2.rpc
import pickle
import six
from . import types


def from_thrift(thrift, spec, result):
    if not spec.thrift_spec:
        assert result is None, repr(type(result))
        return result
    if 0 not in spec.thrift_spec:
        return result
    t = spec.thrift_spec[0]
    if t[0] == thr.thrift.TType.STRING:
        assert isinstance(result, str), repr(type(result))
        return result
    elif t[0] in [
            thr.thrift.TType.I08,
            thr.thrift.TType.I16,
            thr.thrift.TType.I32,
            thr.thrift.TType.I64]:
        assert isinstance(result, int), repr(type(result))
        return result
    elif t[0] == thr.thrift.TType.BOOL:
        assert isinstance(result, bool), repr(type(result))
        return result
    elif t[0] == thr.thrift.TType.DOUBLE:
        assert isinstance(result, float), repr(type(result))
        return result
    elif t[0] == thr.thrift.TType.SET:
        return set(result)
    elif t[0] == thr.thrift.TType.LIST:
        assert isinstance(result, list), repr(type(result))
        return result
    elif t[0] == thr.thrift.TType.MAP:
        assert isinstance(result, dict), repr(type(result))
        return result
    elif t[0] == thr.thrift.TType.STRUCT:
        # _i, status, tcls, _b = t
        # assert t[1] == 'success', repr(t)
        # assert isinstance(result, t[2]), repr(type(result))
        return types.toobject(thrift, result)

    raise NotImplementedError(repr((t, type(result))))


class Client(object):
    """Thrift multiplex client.

    The thrift client loads the thrift configuration from a thrift
    server.
    """

    def __init__(self, thrift_content=None, **options):
        self.thrift_content = thrift_content
        self.multiplexed = options.pop('multiplexed', True)
        self.thrift_content_service = options.pop(
            'thrift_content_service', 'info')
        self.options = options
        self._update_thrift()

    def _update_thrift(self):
        """Update client thrift configuration from server.

        The servers thrift configuration must contain at least

          service info { string thrift_content() }

        and the server thrift must run in multiplexed mode.
        """
        if self.thrift_content is None:
            i, fn = tempfile.mkstemp(suffix='.thrift', prefix='rpc-client0-')
            f = os.fdopen(i, mode='w')
            f.write('service %s { string thrift_content() }'
                    % (self.thrift_content_service))
            f.close()
            tmp_thrift = thr.load(fn)
            os.remove(fn)
            if not self.multiplexed:
                factory = thr.protocol.TBinaryProtocolFactory()
            else:
                factory = thr.protocol.TMultiplexedProtocolFactory(
                    thr.protocol.TBinaryProtocolFactory(),
                    self.thrift_content_service)
            service = getattr(tmp_thrift, self.thrift_content_service)
            ctx = thr.rpc.client_context(service,
                                         proto_factory=factory,
                                         **self.options)
            with ctx as c:
                self.thrift_content = c.thrift_content()
        i, fn = tempfile.mkstemp(suffix='.thrift', prefix='rpc-client-')
        f = os.fdopen(i, mode='w')
        f.write(self.thrift_content)
        f.close()
        self.thrift = thr.load(fn)
        os.remove(fn)

    def _args_to_thrift(self, spec, args):
        thrift_spec = spec.thrift_spec
        thrift_args = []
        for i, arg in enumerate(args):
            t = thrift_spec[i+1]
            if t[0] == thr.thrift.TType.STRING:
                arg = types.fromobject(self.thrift, str, arg)
            elif t[0] == thr.thrift.TType.STRUCT:
                arg = types.fromobject(self.thrift, t[2], arg)
            elif t[0] in [
                    thr.thrift.TType.I08,
                    thr.thrift.TType.I16,
                    thr.thrift.TType.I32,
                    thr.thrift.TType.I64]:
                arg = types.fromobject(self.thrift, int, arg)
            elif t[0] == thr.thrift.TType.BOOL:
                arg = types.fromobject(self.thrift, bool, arg)
            elif t[0] == thr.thrift.TType.DOUBLE:
                arg = types.fromobject(self.thrift, float, arg)
            elif t[0] == thr.thrift.TType.SET:
                arg = types.fromobject(self.thrift, set, arg)
            elif t[0] == thr.thrift.TType.LIST:
                arg = types.fromobject(self.thrift, list, arg)
            elif t[0] == thr.thrift.TType.MAP:
                arg = types.fromobject(self.thrift, dict, arg)
            else:
                raise NotImplementedError(repr((t, type(arg))))
            thrift_args.append(arg)
        return tuple(thrift_args)

    def _result_from_thrift(self, spec, result):
        return from_thrift(self.thrift, spec, result)

    def __call__(self, **services):
        """Perform a RPC call to thrift server.

        Parameters
        ----------
        services : dict
          Specify a service mapping with arguments to server methods.

        Returns
        -------
        results : dict
          The return values of server methods organized in a service
          mapping. See example below.

        Example
        -------

        1. Create a connection to thrift server:

          conn = Client(host=.., port=..)

        2. Say, a server implements two services, Ping and King. Ping
        service has a method `bong(i) -> int` and King has a method
        `kong(s) -> str`. To call these methods from a client, use

          results = conn(Ping=dict(bong=(5,)), King=dict(kong=('hello')))

        The `results` will be, for instance,

          dict(Ping=dict(bong=6), King=dict(kong='hi'))

        where `bong(5) -> 6` and `kong('hello') -> 'hi'` is assumed.
        """
        exception = getattr(self.thrift, 'Exception', Exception)

        results = {}
        for service_name, query_dict in services.items():
            factory = thr.protocol.TBinaryProtocolFactory()
            if self.multiplexed:
                factory = thr.protocol.TMultiplexedProtocolFactory(
                    factory, service_name)
            service = getattr(self.thrift, service_name)
            ctx = thr.rpc.client_context(service, proto_factory=factory,
                                         **self.options)
            results[service_name] = {}
            with ctx as c:
                for query_name, query_args in query_dict.items():
                    argsmth = getattr(service, query_name + '_args')
                    query_args = self._args_to_thrift(argsmth, query_args)
                    mth = getattr(c, query_name)
                    assert mth is not None
                    exc = None
                    try:
                        r = mth(*query_args)
                    except exception as msg:
                        if exception is Exception:
                            raise
                        if msg.kind == self.thrift.ExceptionKind.EXC_TBLIB:
                            exc = pickle.loads(msg.message)
                        elif msg.kind == self.thrift.ExceptionKind.EXC_MESSAGE:
                            et, ev = msg.message.split(':', 1)
                            et = __builtins__.get(et)
                            if et is None:
                                et = Exception
                                ev = msg.message
                            exc = et, et(ev.lstrip()), None
                        else:
                            raise
                    except Exception as msg:
                        print(msg)
                        raise
                    if exc is not None:
                        six.reraise(*exc)   # RAISING SERVER EXCEPTION
                    r = self._result_from_thrift(
                        getattr(service, query_name + '_result'), r)
                    results[service_name][query_name] = r
        return results
