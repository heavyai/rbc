"""Server implements multiplex thrift server, that is, there may
exists multiple thrift services in the thrift configurarion files.

Server features are defined by a Disparcher class that implements the
methods that are triggered by clients queries, and a thrift
configuration files that define available services and the methods.
"""
# Author: Pearu Peterson
# Created: February 2019


import os
import tempfile
import socket
import time
import multiprocessing
import sys
import pickle
from . import utils

import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import thriftpy2 as thr
    import thriftpy2.rpc

try:
    import tblib
    import tblib.pickling_support
except ImportError:
    tblib = None

if tblib is not None:
    tblib.pickling_support.install()


class TServerSocket(thr.transport.TServerSocket):

    def close(self):
        # Copied from https://github.com/Thriftpy/thriftpy2/pull/184
        # Remove when using pythrift2 > 0.4.14
        if not self.sock:
            return

        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

        try:
            self.sock.close()
        except OSError:
            pass
        self.sock = None


class Processor(thr.thrift.TProcessor):

    def __init__(self, server, service, handler):
        self.server = server
        thr.thrift.TProcessor.__init__(self, service, handler)

    def handle_exception(self, e, result):
        if thr.thrift.TProcessor.handle_exception(self, e, result):
            return True
        # map Python native exception to thrift Exception so that
        # client can remap thrift Exception to Python

        exc_type = self.server.thrift.Exception
        if tblib is not None:
            exc_kind = self.server.thrift.ExceptionKind.EXC_TBLIB
            exc = exc_type(exc_kind, pickle.dumps(sys.exc_info()))
        else:
            # tblib would be required to pickle traceback instances
            exc_kind = self.server.thrift.ExceptionKind.EXC_MESSAGE
            et, ev, tb = sys.exc_info()
            exc = exc_type(exc_kind, '%s: %s' % (et.__name__, ev))
        return thr.thrift.TProcessor.handle_exception(self, exc, result)


class MultiplexedProcessor(thr.thrift.TMultiplexedProcessor):

    def __init__(self, server):
        self.server = server
        thr.thrift.TMultiplexedProcessor.__init__(self)

    def handle_exception(self, e, result):
        if thr.thrift.TProcessor.handle_exception(self, e, result):
            return True
        # map Python native exception to thrift Exception so that
        # client can remap thrift Exception to Python

        exc_type = self.server.thrift.Exception
        if tblib is not None:
            exc_kind = self.server.thrift.ExceptionKind.EXC_TBLIB
            exc = exc_type(exc_kind, pickle.dumps(sys.exc_info()))
        else:
            # tblib would be required to pickle traceback instances
            exc_kind = self.server.thrift.ExceptionKind.EXC_MESSAGE
            et, ev, tb = sys.exc_info()
            exc = exc_type(exc_kind, '%s: %s' % (et.__name__, ev))
        return thr.thrift.TProcessor.handle_exception(self, exc, result)


class Server(object):
    """Multiplex thrift server
    """

    def __init__(self, dispatcher, thrift_file, **options):
        self.multiplexed = options.pop('multiplexed', True)
        self.thrift_content_service = options.pop(
            'thrift_content_service', 'info')
        thrift_content = options.pop('thrift_content', None)
        self.debug = options.pop('debug', False)
        self.options = options
        module_name = os.path.splitext(
            os.path.abspath(thrift_file))[0]+'_thrift'
        self._dispatcher = dispatcher
        self.thrift_file = thrift_file
        if thrift_content is None:
            thrift_content = utils.resolve_includes(
                open(thrift_file).read(), [os.path.dirname(thrift_file)])
        i, fn = tempfile.mkstemp(suffix='.thrift', prefix='rpc-server-')
        f = os.fdopen(i, mode='w')
        f.write(thrift_content)
        f.close()
        self.thrift = thr.load(fn, module_name=module_name)
        os.remove(fn)

    @staticmethod
    def run(dispatcher, thrift_file, options):
        """Run server in current process.
        """
        Server(dispatcher, thrift_file, **options)._serve()

    @staticmethod
    def run_bg(dispatcher, thrift_file, options, startup_time=5):
        """Run server in background process.
        """
        ctx = multiprocessing.get_context('spawn')
        p = ctx.Process(target=Server.run,
                        args=(dispatcher, thrift_file, options))
        p.start()
        start = time.time()
        number_of_tries = 0
        while time.time() < start + startup_time:
            number_of_tries += 1
            try:
                socket.create_connection(
                    (options['host'], options['port']), timeout=0.2)
            except ConnectionRefusedError:
                time.sleep(0.5)
            except Exception as msg:
                warnings.warn(f'Connection failed: `{msg}`, trying again in 0.5 secs..')
                time.sleep(0.5)
            else:
                break
        else:
            is_alive = p.is_alive()
            if is_alive:
                p.join(1)
                p.terminate()
            raise RuntimeError(
                'failed to start up rpc_thrift server'
                f' (was alive={is_alive}, startup time={startup_time}s,'
                f' elapsed time={time.time() - start})')
        if number_of_tries > 1:
            warnings.warn(
                f'More than one try ({number_of_tries}) in starting up rpc_thrift_server.'
                f' Total elapsed time is {time.time() - start} seconds.')
        return p

    def _serve(self):
        """Create and run a Thrift server.
        """
        if self.multiplexed:
            service_names = [_n for _n in dir(self.thrift)
                             if not _n.startswith('_')]
            s_proc = MultiplexedProcessor(self)
            for service_name in service_names:
                service = getattr(self.thrift, service_name)
                proc = thr.thrift.TProcessor(service, self._dispatcher(self, debug=self.debug))
                s_proc.register_processor(service_name, proc)
        else:
            service = getattr(self.thrift, self.thrift_content_service)
            s_proc = Processor(self, service, self._dispatcher(self, debug=self.debug))
        server = thr.server.TThreadedServer(
            s_proc,
            TServerSocket(**self.options),
            iprot_factory=thr.protocol.TBinaryProtocolFactory(),
            itrans_factory=thr.transport.TBufferedTransportFactory())
        server.serve()
