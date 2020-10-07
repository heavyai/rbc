import os
import ctypes
import ctypes.util
from enum import IntEnum
from .utils import runcommand

#
# Import CUDA RT library
#

if os.name == 'nt':
    lib = ctypes.util.find_library('libcudart')
    if lib is None:
        lib = ctypes.util.find_library('libcudart.dll')
else:
    lib = ctypes.util.find_library('cudart')

if lib is None:
    raise ImportError('Failed to find CUDA RT library.'
                      ' Make sure that the library is installed')


def get_cuda_libdir():
    for line in runcommand('ldconfig', '-p').splitlines():
        if 'cudart' in line:
            libpath = os.path.realpath(line.split('=>')[-1].strip())
            return os.path.dirname(libpath)
    return os.path.dirname(lib)


if not os.path.isfile(lib):
    lib = os.path.join(get_cuda_libdir(), lib)

libdir = os.path.dirname(lib)
includedir = os.path.join(os.path.dirname(libdir), 'include')

libcudart = ctypes.cdll.LoadLibrary(lib)

#
# Auxiliary functions for reading CUDA header files
#


def get_cuda_header(headername, _cache={}):
    content = _cache.get(headername)
    if content is None:
        content = open(os.path.join(includedir, headername)).read()
        _cache[headername] = content
    return content


def read_cuda_header(headername, kind, name, process_func):
    content = get_cuda_header(headername)
    flag = False
    d = {}
    key = -1
    for line in content.splitlines():
        line = line.strip()
        if not flag and line.startswith(kind) and line.endswith(name):
            flag = True
            comment = False
        elif flag:
            if line.startswith('}'):
                break
            if comment:
                j = line.find('*/')
                if j != -1:
                    comment = False
                    line = line[j+2:]
                else:
                    continue
            else:
                i = line.find('/*')
                if i != -1:
                    j = line.find('*/')
                    if j != -1:
                        line = line[:i] + line[j+2:]
                    else:
                        comment = True
                        continue

            line = line.strip()
            if line.endswith(',') or line.endswith(';'):
                line = line[:-1].rstrip()
            if not line or line == '{':
                continue
            key, value = process_func(line, key)
            if value is None:
                continue
            d[key] = value
    return d


def get_cuda_enum(headerfile, name):
    """Return value:name mapping of a enum definition.
    """
    def process_enum(line, last_value):
        if '=' in line:
            n, v = line.split('=')
            n = n.strip()
            v = eval(v.strip(), {}, {})
        else:
            n = line
            v = last_value + 1
        return v, n
    return read_cuda_header(headerfile, 'enum', name, process_enum)


def get_cuda_struct(headerfile, name):
    """Return membername:type mapping of a struct definition.
    """
    def process_struct(line, last_value):
        typ, decl = line.rsplit(' ', 1)
        typ = typ.strip()
        t = get_ctype(typ)
        i = decl.find('[')
        if i != -1:
            dim = int(decl[i+1:-1])
            t = t * dim
            n = decl[:i]
        else:
            n = decl
        return n, t
    return read_cuda_header(headerfile, 'struct', name, process_struct)


def get_ctype(tstr):
    t = getattr(ctypes, 'c_' + tstr, None)
    if t is not None:
        return t
    t = {'cudaUUID_t': cudaUUID_t,
         'unsigned int': ctypes.c_uint}.get(tstr)
    if t is not None:
        return t
    raise NotImplementedError('get ctypes version of `%s`' % (tstr))


#
# CUDA enum definitions
#


class CTypesEnum(IntEnum):

    # See
    #   https://www.chriskrycho.com/2015/ctypes-structures-and-dll-exports.html
    @classmethod
    def from_param(cls, obj):
        return int(obj)


cudaError_value_name_map = get_cuda_enum('driver_types.h', 'cudaError')
cudaError_name_value_map = dict((n, v)
                                for (v, n) in cudaError_value_name_map.items())
_cudaError_member_defs = '\n'.join(
    ['%s = %s' % _item for _item in cudaError_name_value_map.items()])


class cudaError(CTypesEnum):
    exec(_cudaError_member_defs)


#
# CUDA struct definitions
#


class cudaUUID_t(ctypes.Structure):
    _fields_ = [('bytes', ctypes.c_char * 16)]


class cudaDeviceProp(ctypes.Structure):
    _fields_ = list(get_cuda_struct('driver_types.h',
                                    'cudaDeviceProp').items())


#
# CUDA functions
#

libcudart.cudaDriverGetVersion.argtypes = [ctypes.POINTER(ctypes.c_int)]
libcudart.cudaDriverGetVersion.restype = cudaError

libcudart.cudaRuntimeGetVersion.argtypes = [ctypes.POINTER(ctypes.c_int)]
libcudart.cudaRuntimeGetVersion.restype = cudaError

libcudart.cudaGetDeviceProperties.argtypes = [ctypes.POINTER(cudaDeviceProp),
                                              ctypes.c_int]
libcudart.cudaGetDeviceProperties.restype = cudaError

libcudart.cudaGetDeviceCount.argtypes = [ctypes.POINTER(ctypes.c_int)]
libcudart.cudaGetDeviceCount.restype = cudaError

#
# Convinience functions
#


def get_cuda_versions():
    """Return CUDA driver and runtime versions.
    """
    dr = ctypes.c_int(0)
    rt = ctypes.c_int(0)
    r = libcudart.cudaDriverGetVersion(ctypes.byref(dr))
    assert r == cudaError.cudaSuccess, repr(r)
    if dr.value != 0:
        # driver is installed
        r = libcudart.cudaRuntimeGetVersion(ctypes.byref(rt))
        assert r in [cudaError.cudaSuccess,
                     cudaError.cudaErrorNoDevice], repr(r)
    return dr.value, rt.value


def get_cuda_device_properties(device):
    """Return CUDA device properties as a dictionary.

    The value types in the dictionary will be int, bytes, or list.
    """
    v = cudaDeviceProp()
    d = ctypes.c_int(0)
    r = libcudart.cudaGetDeviceProperties(ctypes.byref(v), d)
    assert r in [cudaError.cudaSuccess, cudaError.cudaErrorNoDevice], repr(r)
    d = {}
    for (n, t) in cudaDeviceProp._fields_:
        a = getattr(v, n)
        if isinstance(a, ctypes.Array):
            a = list(a)
        d[n] = a
    return d


def get_device_count():
    """Return the number of CUDA devices.
    """
    c = ctypes.c_int(0)
    r = libcudart.cudaGetDeviceCount(ctypes.byref(c))
    if r == cudaError.cudaErrorNoDevice:
        return 0
    assert r == cudaError.cudaSuccess, repr(r)  # noqa: F821
    return c.value
