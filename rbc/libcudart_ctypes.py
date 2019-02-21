import os
import ctypes
import ctypes.util
import re
from enum import IntEnum
from .utils import runcommand



if os.name == 'nt':
    lib = ctypes.util.find_library('libcudart')
    if lib is None:
        lib = ctypes.util.find_library('libcudart.dll')
else:
    lib = ctypes.util.find_library('cudart')

if lib is None:
    raise ImportError('Failed to find CUDA RT library. Make sure that the library is installed')

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

def get_cuda_header(headername, _cache={}):
    content = _cache.get(headername)
    if content is None:
        content = open(os.path.join(includedir, headername)).read()
        _cache[headername] = content
    return content

def get_cuda_enum(name, _cache={}):
    """Return reverse map of enum definition.
    """
    content = get_cuda_header('driver_types.h')
    flag = False
    d = {}
    last_value = -1

    for line in content.splitlines():
        line = line.strip()
        if not flag and line.startswith('enum') and line.endswith(name):
            flag = True
            comment = False
        elif flag:
            if line.startswith('}'):
                break
            i = line.find('/*')
            if i != -1:
                line = line[:i]
                comment = True
            if comment:
                j = line.find('*/')
                if j != -1:
                    comment = False
                    if i != -1:
                        line = line[i:j+1]
                    else:
                        continue
                else:
                    continue
            line = line.strip()            
            if line.endswith(','):
                line = line[:-1].rstrip()
            if not line:
                continue
            if '=' in line:
                n, v = line.split('=')
                n = n.strip()
                v = int(v.strip())
            else:
                n = line
                v = last_value + 1
            last_value = v
            d[v] = n
    return d

class CTypesEnum(IntEnum):
    # See https://www.chriskrycho.com/2015/ctypes-structures-and-dll-exports.html
    @classmethod
    def from_param(cls, obj):
        return int(obj)

cudaError_value_name_map = get_cuda_enum('cudaError')
s = 'class cudaError(CTypesEnum):\n'
for v, n in cudaError_value_name_map.items():
    s += '    {} = {}\n'.format(n, v)
exec(s)


libcudart.cudaDriverGetVersion.argtypes = [ctypes.POINTER(ctypes.c_int)]
libcudart.cudaDriverGetVersion.restype = cudaError

libcudart.cudaRuntimeGetVersion.argtypes = [ctypes.POINTER(ctypes.c_int)]
libcudart.cudaRuntimeGetVersion.restype = cudaError

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
        assert r == cudaError.cudaSuccess, repr(r)
    return dr.value, rt.value

print(get_cuda_versions())
