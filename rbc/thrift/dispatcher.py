"""Dispatcher class implements RPC methods that are executed on the
server.
"""
# Author: Pearu Peterson
# Created: February 2019

import os
from .utils import resolve_includes
from ..utils import runcommand


class Dispatcher(object):
    """Default implementation of a dispatcher.
    """

    def __init__(self, server):
        self.server = server

    @property
    def thrift(self):
        return self.server.thrift

    def thrift_content(self):
        return resolve_includes(open(self.server.thrift_file).read(),
                                [os.path.dirname(self.server.thrift_file)])

    def nvidia_smi_query(self):
        return runcommand('nvidia-smi', '-q')


class DispatcherTargets(Dispatcher):

    def get_triplet(self, target):
        if target == 'host':
            return runcommand('llvm-config', '--host-target')
        if target == 'cuda32':
            return 'nvptx-nvidia-cuda'
        if target in ['cuda', 'cuda64']:
            return 'nvptx64-nvidia-cuda'
        raise NotImplementedError('get triplet for target=`%s`' % (target))

    def get_processor_info(self, target):
        """Return information dictionary about processor target.
        """
        if target == 'host':
            return open('/proc/cpuinfo').read()
        if target.startswith('cuda'):
            return runcommand('nvidia-smi', '-q')
        raise NotImplementedError('get processor info for target=`%s`'
                                  % (target))
