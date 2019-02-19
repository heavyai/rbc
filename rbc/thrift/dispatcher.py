"""Dispatcher class implements RPC methods that are executed on the
server.
"""
# Author: Pearu Peterson
# Created: February 2019

import os
import numpy as np
import subprocess

from .utils import resolve_includes

class Dispatcher(object):
    """Default implementation of a dispatcher.
    """
    
    def __init__(self, server):
        self.server = server
        self.thrift = server.thrift

    def thrift_content(self):
        return resolve_includes(open(self.server.thrift_file).read(),
                                [os.path.dirname(self.server.thrift_file)])

    def nvidia_smi_query(self):
        result = subprocess.run(['nvidia-smi', '-q'], stdout=subprocess.PIPE)
        return result.stdout
