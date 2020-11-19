import ctypes
import json
from . import libfuncs
from .utils import parse_version


class TargetInfo(object):
    """Holds target device information.
    """

    @classmethod
    def set_instance(cls, instance):
        cls._instance = instance

    @classmethod
    def instance(cls):
        assert cls._instance is not None, "No TargetInfo instance was set prior to this point."
        return cls._instance

    @classmethod
    def clear_instance(cls):
        cls._instance = None

    def __init__(self, name, strict=False):
        """
        Parameters
        ----------
        name : str
          Specify unique name of a target device.
        strict: bool
          When True, require that atomic types are concrete. Used by
          typesystem.
        """
        self.name = name
        self.strict = strict
        self.custom_type_converters = []
        self.info = {}
        self.type_sizeof = {}
        self._supported_libraries = set()  # libfuncs.Library instances

    def add_library(self, lib):
        if isinstance(lib, str):
            lib = libfuncs.Library.get(lib)
        if isinstance(lib, libfuncs.Library):
            self._supported_libraries.add(lib)
        else:
            raise TypeError(
                f'Expected libfuncs.Library instance or library name but got {type(lib)}')

    def supports(self, name):
        """Return True if the target system defines symbol name.
        """
        for lib in self._supported_libraries:
            if name in lib:
                return True
        return False

    def todict(self):
        return dict(name=self.name, strict=self.strict, info=self.info,
                    type_sizeof=self.type_sizeof,
                    libraries=[lib.name for lib in self._supported_libraries])

    @classmethod
    def fromdict(cls, data):
        target_info = cls(data.get('name', 'somedevice'),
                          strict=data.get('strict', False))
        target_info.info.update(data.get('info', {}))
        target_info.type_sizeof.update(data.get('type_sizeof', {}))
        for lib in data.get('libraries', []):
            target_info.add_library(lib)
        return target_info

    def tojson(self):
        return json.dumps(self.todict())

    @classmethod
    def fromjson(cls, data):
        return cls.fromdict(json.loads(data))

    _host_target_info_cache = {}

    @classmethod
    def host(cls, name='host_cpu', strict=False):
        """Return target info for host CPU.
        """
        key = (name, strict)

        target_info = cls._host_target_info_cache.get(key)
        if target_info is not None:
            return target_info

        import llvmlite.binding as ll
        target_info = TargetInfo(name=name, strict=strict)
        target_info.set('name', ll.get_host_cpu_name())
        target_info.set('triple', ll.get_default_triple())
        features = ','.join(['-+'[int(v)] + k
                             for k, v in ll.get_host_cpu_features().items()])
        target_info.set('features', features)

        for tname, ctype in dict(
                bool=ctypes.c_bool,
                size_t=ctypes.c_size_t,
                ssize_t=ctypes.c_ssize_t,
                char=ctypes.c_char,
                uchar=ctypes.c_char,
                schar=ctypes.c_char,
                byte=ctypes.c_byte,
                ubyte=ctypes.c_ubyte,
                wchar=ctypes.c_wchar,
                short=ctypes.c_short,
                ushort=ctypes.c_ushort,
                int=ctypes.c_int,
                uint=ctypes.c_uint,
                long=ctypes.c_long,
                ulong=ctypes.c_ulong,
                longlong=ctypes.c_longlong,
                ulonglong=ctypes.c_ulonglong,
                float=ctypes.c_float,
                double=ctypes.c_double,
                longdouble=ctypes.c_longdouble,
        ).items():
            target_info.type_sizeof[tname] = ctypes.sizeof(ctype)

        target_info.add_library('m')
        target_info.add_library('stdio')
        target_info.add_library('stdlib')

        cls._host_target_info_cache[key] = target_info

        return target_info

    def set(self, prop, value):
        """Set a target device property to given value.
        """
        supported_keys = ('name', 'triple', 'datalayout', 'features', 'bits',
                          'compute_capability', 'count', 'threads', 'cores',
                          'has_cpython', 'has_numba', 'driver', 'software')
        if prop not in supported_keys:
            print(f'rbc.{type(self).__name__}:'
                  f' unsupported property {prop}={value}.')
        self.info[prop] = value

    # Convenience methods

    @property
    def software(self):
        """Return remote software name and version number as int tuple.
        """
        lst = self.info.get('software', '').split(None, 1)
        if len(lst) == 2:
            name, version = lst
        else:
            return lst[0], ()
        return name, parse_version(version)

    @property
    def driver(self):
        """Return device driver name and version numbers as int tuple. For
        instance::

          'CUDA', (11, 0)
        """
        lst = self.info.get('driver', '').split(None, 1)
        if len(lst) == 2:
            name, version = lst
        else:
            return lst[0], ()
        return name, parse_version(version)

    @property
    def triple(self):
        """Return target triple as a string.

        The triple is in a form "<arch>-<vendor>-<os>"
        """
        return self.info['triple']

    @property
    def arch(self):
        """Return architecture string of target device.
        """
        return self.triple.split('-', 1)[0]

    @property
    def bits(self):
        """Return target device address bit-size as int value (32, 64, ...).
        """
        bits = self.info.get('bits')
        if bits is not None:
            return bits
        # expand this dict as needed
        return dict(x86_64=64, nvptx64=64,
                    x86=32, nvptx=32)[self.arch]

    @property
    def datalayout(self):
        """Return LLVM datalayout of target device.
        """
        layout = self.info.get('datalayout')
        if layout is None:
            if 'cpu' not in self.name:
                print(f'rbc.{type(self).__name__}:'
                      f' no datalayout info for {self.name!r} device')
            # In the following we assume that datalayout of the target
            # host matches with the datalayout of the client host,
            # regardless of what OS these hosts run.
            layout = ''
        return layout

    @property
    def device_features(self):
        """Return a comma-separated string of CPU target features.
        """
        if 'features' in self.info:
            return ','.join(self.info['features'].split())
        return ''

    @property
    def gpu_cc(self):
        """Return compute capabilities (major, minor) of CUDA device target.
        """
        return tuple(map(int, self.info['compute_capability'].split('.')))

    @property
    def device_name(self):
        """Return the name of target device.
        """
        return self.info['name']

    @property
    def is_cpu(self):
        """Return True if the target device is CPU.
        """
        return 'cpu' in self.name

    @property
    def is_gpu(self):
        """Return True if the target device is GPU.
        """
        return 'gpu' in self.name

    @property
    def has_numba(self):
        """Check if target supports numba symbols
        """
        return self.info.get('has_numba', False)

    @property
    def has_numpy(self):
        """Check if target supports numpy symbols
        """
        return self.info.get('has_numpy', False)

    @property
    def has_cpython(self):
        """Check if target supports Python C/API symbols
        """
        return self.info.get('has_cpython', False)

    # TODO: info may also contain: count, threads, cores

    # Worker methods

    def sizeof(self, t):
        """Return the sizeof(t) value for given target device.

        Parameters
        ----------
        t : {str, ...}
            Specify types name. For complete support, one should
            implement the sizeof for the following type names: char,
            uchar, schar, byte, ubyte, short, ushort, int, uint, long,
            ulong, longlong, ulonglong, float, double, longdouble,
            complex, bool, size_t, ssize_t, wchar.

        Returns
        -------
        size : int
          Byte-size of the input type.
        """
        s = self.type_sizeof.get(t)
        if s is not None:
            return s
        if isinstance(t, str):
            if t == 'complex':
                return self.sizeof('float') * 2
            if t == 'double':
                return 8
            if t == 'float':
                return 4
            if t == 'int':
                return 4
            if t == 'size_t':
                return self.bits // 8
        if isinstance(t, type) and issubclass(t, ctypes._SimpleCData):
            return ctypes.sizeof(t)
        raise NotImplementedError(
            f"{type(self).__name__}[{self.name},{self.triple}].sizeof({t!r})")

    def add_converter(self, converter):
        """Add custom type converter.

        Custom type converters are called on non-concrete atomic
        types.

        Parameters
        ----------
        converter : callable
          Specify a function with signature `converter(target_info,
          obj)` that returns `Type` instance corresponding to
          `obj`. If the conversion is unsuccesful, the `converter`
          returns `None` so that other converter functions could be
          tried.
        """
        self.custom_type_converters.append(converter)

    def custom_type(self, t):
        """Return custom type of an object.
        """
        for converter in self.custom_type_converters:
            r = converter(self, t)
            if r is not None:
                return r
