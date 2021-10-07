"""This module implements the bi-directional conversations between
objects defined by thrift module and Python objects.

One can use class instances Buffer, NDArray as annotations in the
definitions of Dispatcher methods that are decorated with
dispatchermethod.

"""
# Author: Pearu Peterson
# Created: February 2019

import ctypes
import _ctypes
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


class TypeData(tuple):
    """Holds generated type for pickling:
    - constructor of the type
    - name of generated type
    - picklable data required to construct the type
    """
    @classmethod
    def fromctypes(cls, typ):
        if issubclass(typ, _ctypes.Structure):
            fields = _prepickle_dumps(tuple(typ._fields_))
            return cls((ctypes.Structure, typ.__name__, (('_fields_', fields),)))
        if issubclass(typ, _ctypes._Pointer):
            return cls((ctypes.POINTER, typ.__name__, _prepickle_dumps(typ._type_)))
        raise NotImplementedError(repr((type(typ), typ)))

    def toctypes(self):
        constructor, typname, typedata = self
        if isinstance(constructor, type) and issubclass(constructor, _ctypes.Structure):
            return type(typname, (constructor,), dict(_postpickle_loads(typedata)))
        if constructor is ctypes.POINTER:
            typ = ctypes.POINTER(_postpickle_loads(typedata))
            assert typ.__name__ == typname, (typ.__name__, typname)
            return typ
        raise NotImplementedError(repr(self))


class PointerData(tuple):
    """Holds pointer data for pickling.

    PointerData is a 4-tuple containing:
    - pointer value as int
    - value type as ctypes type
    - pointer level as int
    - sizeof value type (for integrity check)
    """

    @classmethod
    def fromctypes(cls, ptr):
        if isinstance(ptr, _ctypes._Pointer):
            level = 1   # ANSI C requires support for 12 levels as a maximum
            t = ptr._type_
            while issubclass(t, ctypes._Pointer):
                level += 1
                t = t._type_
            if t == ctypes.c_void_p:
                level += 1
            value = ctypes.cast(ptr, ctypes.c_void_p).value
            td = _prepickle_dumps(t)
            return cls((value,
                        td,
                        level,
                        ctypes.sizeof(t)))
        if isinstance(ptr, ctypes.c_void_p):
            t = type(ptr)
            value = ptr.value
            return cls((value,
                        t,
                        1,
                        ctypes.sizeof(t)))
        raise NotImplementedError(repr((type(ptr), ptr)))

    def toctypes(self):
        value, dtype, level, sizeof_dtype = self
        dtype = _postpickle_loads(dtype)
        assert ctypes.sizeof(dtype) == sizeof_dtype, (
            dtype, ctypes.sizeof(dtype), sizeof_dtype)  # TODO: mismatch of type sizes!
        if dtype == ctypes.c_void_p:
            t = dtype
        else:
            t = ctypes.POINTER(dtype)
        for i in range(1, level):
            t = ctypes.POINTER(t)
        ptr = ctypes.c_void_p(value)
        if t != ctypes.c_void_p:
            ptr = ctypes.cast(ptr, t)
        return ptr


class StructData(tuple):
    """Holds structure data for pickling.

    StructData is a 2-tuple containing:
    - structure type with _prepickle_dumps applied
    - tuple member values with _prepickle_dumps applied
    """

    @classmethod
    def fromctypes(cls, obj):
        if isinstance(obj, _ctypes.Structure):
            values = tuple([getattr(obj, name) for name, typ in obj._fields_])
            return cls(_prepickle_dumps(type(obj), values))
        raise NotImplementedError(repr((type(obj), obj)))

    def toctypes(self):
        cls, values = _postpickle_loads(self)
        return cls(*values)


def _prepickle_dumps(data):
    if isinstance(data, (_ctypes._Pointer, ctypes.c_void_p)):
        return PointerData.fromctypes(data)
    if isinstance(data, _ctypes.Structure):
        return StructData.fromctypes(data)
    if isinstance(data, tuple):
        return tuple(map(_prepickle_dumps, data))
    if isinstance(data, type) and issubclass(data, (_ctypes.Structure, _ctypes._Pointer)):
        return TypeData.fromctypes(data)
    return data


def _postpickle_loads(data):
    if isinstance(data, (PointerData, StructData, TypeData)):
        return data.toctypes()
    if isinstance(data, tuple):
        return tuple(map(_postpickle_loads, data))
    return data


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
            r.data = pickle.dumps(_prepickle_dumps(data))
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
            return _postpickle_loads(pickle.loads(obj.data))
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
