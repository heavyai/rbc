"""This module implements the bi-directional conversations between
objects defined by thrift module and Python objects.

One can use class instances Buffer, NDArray as annotations in the
definitions of Dispatcher methods that are decorated with
dispatchermethod.

"""
# Author: Pearu Peterson
# Created: February 2019

import pickle
import numpy as np


def fromobject(thrift, tcls, obj):
    """Create thrift object with type `tcls` from a Python object.
    """
    if isinstance(obj, tcls):
        return obj
    return globals().get(tcls.__name__, tcls)(thrift, obj)


def toobject(thrift, tobj, cls=None):
    """Convert thrift object `tobj` to Python object (with optional type
    `cls`).
    """
    if cls is not None and isinstance(tobj, cls):
        return tobj
    tcls = globals().get(type(tobj).__name__)
    if tcls is not None:
        return tcls.toobject(thrift, tobj, cls=cls)
    if cls is not None:
        return cls(tobj)
    return tobj


class Data(object):
    """Represents any data that interpretation is defined by info and kind
    attributes.

    """
    def __new__(cls, thrift, data, info=None):
        r = thrift.Data()
        if isinstance(data, str):
            r.data = data.encode()
            r.kind = thrift.DataKind.DATA_ENCODED
            r.info = 'utf-8' if info is None else info
        elif isinstance(data, bytes):
            r.data = data
            r.kind = thrift.DataKind.DATA_RAW
            r.info = ''
        else:
            # protocol = pickle.DEFAULT_PROTOCOL if info is None else int(info)
            r.data = pickle.dumps(data)
            r.kind = thrift.DataKind.DATA_PICKLED
            r.info = str(info).encode()
        return r

    @staticmethod
    def toobject(thrift, obj, cls=None):
        kind = obj.kind
        if kind == thrift.DataKind.DATA_ENCODED:
            encoding = obj.info
            if isinstance(obj.data, str):
                return obj.data
            return obj.data.decode(encoding)
        if kind == thrift.DataKind.DATA_RAW:
            return obj.data
        if kind == thrift.DataKind.DATA_PICKLED:
            # protocol = int(obj.info)  # pickle.loads detects protocol itself
            return pickle.loads(obj.data)
        raise NotImplementedError(repr(kind))


class Buffer(object):
    """Represents a buffer that is mapped to/from Python bytes.
    """
    def __new__(cls, thrift, data):
        r = thrift.Buffer()
        if isinstance(data, thrift.Buffer):
            data = data.data
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, np.ndarray):
            data = data.tobytes()
        if not isinstance(data, bytes):
            raise NotImplementedError('Buffer from {}'.format(type(data)))
        r.data = data
        return r

    @staticmethod
    def toobject(thrift, obj, cls=None):
        data = obj.data
        if isinstance(data, str):
            if cls is str:
                return data
            data = data.encode()
        if cls is bytes:
            return data
        if cls is np.ndarray or cls is None:
            return np.frombuffer(data, dtype=np.uint8)
        raise NotImplementedError('Buffer to {}'.format(cls))


class NDArray(object):
    """Represents N-dimensional array that is mapped to/from numpy
    ndarray.
    """

    def __new__(cls, thrift, data):
        r = thrift.NDArray()
        if isinstance(data, thrift.NDArray):
            r.data = data.data
            r.typestr = data.typestr
            r.shape = data.shape
        elif isinstance(data, np.ndarray):
            r.shape = list(data.shape)
            r.typestr = data.dtype.str
            r.data = data.tobytes()   # ensures C-contiguous array
        else:
            raise NotImplementedError('Buffer from {}'.format(type(data)))
        return r

    @staticmethod
    def toobject(thrift, obj, cls=None):
        shape = tuple(obj.shape)
        dtype = np.dtype(obj.typestr)
        data = obj.data
        if cls is np.ndarray or cls is None:
            if isinstance(data, str):
                data = data.encode()
            # C-contiguous array
            return np.frombuffer(data, dtype=dtype).reshape(shape)
        raise NotImplementedError('NDArray to {}'.format(cls))
