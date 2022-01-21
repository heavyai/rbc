"""TargetInfo class specific
"""
import ctypes
import json
import warnings
from . import libfuncs
from .utils import parse_version


class TargetInfo(object):
    """Holds target device information.

    The information within a TargetInfo instance can be accessed
    only when the instance has been activated as a context manager::

      target_info = TargetInfo(<name>)
      # construct target_info
      ...

      with target_info:
          '''
          TargetInfo instance target_info is made available globally
          and it can be acquired from any Python module function, for
          example:

            from rbc.targetinfo import TargetInfo

            def foo():
                ti = TargetInfo()
                # here `ti` is the same object as `target_info`
          '''
          ...

      # target_info is now disabled globally and when a Python module
      # function tries to access it as `TargetInfo()`, a RuntimeError
      # exception will be raised.
    """

    _instance = None

    @classmethod
    def _set_instance(cls, instance):
        if instance is None:
            assert cls._instance is not None
        else:
            assert cls._instance is None
        cls._instance = instance
        return instance

    @classmethod
    def _get_instance(cls):
        if cls._instance is None:
            raise RuntimeError('Target not specified.')
        return cls._instance

    def __enter__(self):
        if self.nested:
            parent = type(self)._instance
            if parent is not None:
                type(self)._set_instance(None)
                self.update(parent)
            self._parent = parent
        return type(self)._set_instance(self)

    def __exit__(self, exc_type, exc_value, traceback):
        type(self)._set_instance(None)
        if self._parent is not None:
            assert self.nested
            type(self)._set_instance(self._parent)
        if exc_type is None:
            return True

    def __new__(cls, *args, **kwargs):
        """Return a global or a newly constructed TargetInfo instance.
        """
        if not (args or kwargs):
            return cls._get_instance()
        obj = object.__new__(cls)
        obj._init(*args, **kwargs)
        return obj

    def _init(self, name: str, strict: bool = False, nested: bool = False,
              use_tracing_allocator: bool = False):
        """
        Parameters
        ----------
        name : str
          Specify unique name of a target device.
        strict: bool
          When True, require that atomic types are concrete. Used by
          typesystem.
        nested: bool
          When True, allow nested target info contexts.
        use_tracing_allocator: bool
          When True, use the tracing allocator, to enable the LeakDetector
        """
        self.name = name
        self.strict = strict
        self.nested = nested
        self.use_tracing_allocator = use_tracing_allocator
        self._parent = None
        self.info = {}
        self.type_sizeof = {}
        self._supported_libraries = set()  # libfuncs.Library instances
        self._userdefined_externals = set()

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.name!r})'

    def add_external(self, *names):
        self._userdefined_externals.update(names)

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
        if name in self._userdefined_externals:
            return True
        for lib in self._supported_libraries:
            if name in lib:
                return True
        return False

    def todict(self):
        return dict(name=self.name, strict=self.strict,
                    use_tracing_allocator=self.use_tracing_allocator,
                    info=self.info,
                    type_sizeof=self.type_sizeof,
                    libraries=[lib.name for lib in self._supported_libraries],
                    externals=list(self._userdefined_externals))

    @classmethod
    def fromdict(cls, data):
        target_info = cls(data.get('name', 'somedevice'),
                          strict=data.get('strict', False),
                          use_tracing_allocator=data.get('use_tracing_allocator', False))
        target_info.update(data)
        return target_info

    def update(self, data):
        """Update target info using other target info data. Any existing data
        bit will be overwritten except the name and strict fields.
        """
        if isinstance(data, type(self)):
            data = data.todict()
        self.info.update(data.get('info', {}))
        self.type_sizeof.update(data.get('type_sizeof', {}))
        for lib in data.get('libraries', []):
            self.add_library(lib)
        self.add_external(*data.get('externals', []))

    def tojson(self):
        return json.dumps(self.todict())

    @classmethod
    def fromjson(cls, data):
        return cls.fromdict(json.loads(data))

    @classmethod
    def dummy(cls):
        """Returns dummy target info instance.

        Warning: When a target info context already exists, its data
        is copied into the dummy target info except the name, strict
        and nested fields. The existing target info context will be
        restored when leaving the dummy target info context.
        """
        return TargetInfo(name='dummy', strict=False, nested=True)

    _host_target_info_cache = {}

    @classmethod
    def host(cls, name='host_cpu', strict=False, use_tracing_allocator=False):
        """Return target info for host CPU.
        """
        key = (name, strict, use_tracing_allocator)

        target_info = TargetInfo._host_target_info_cache.get(key)
        if target_info is not None:
            return target_info

        import llvmlite.binding as ll
        target_info = cls(name=name, strict=strict,
                          use_tracing_allocator=use_tracing_allocator)
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
                voidptr=ctypes.c_void_p
        ).items():
            target_info.type_sizeof[tname] = ctypes.sizeof(ctype)

        target_info.add_library('m')
        target_info.add_library('stdio')
        target_info.add_library('stdlib')
        target_info.add_library('rbclib')
        if use_tracing_allocator:
            target_info.set('fn_allocate_varlen_buffer',
                            'rbclib_tracing_allocate_varlen_buffer')
            target_info.set('fn_free_buffer',
                            'rbclib_tracing_free_buffer')
        else:
            target_info.set('fn_allocate_varlen_buffer', 'rbclib_allocate_varlen_buffer')
            target_info.set('fn_free_buffer', 'rbclib_free_buffer')
        cls._host_target_info_cache[key] = target_info

        return target_info

    def set(self, prop, value):
        """Set a target device property to given value.
        """
        supported_keys = ('name', 'triple', 'datalayout', 'features', 'bits',
                          'compute_capability', 'count', 'threads', 'cores',
                          'has_cpython', 'has_numba', 'driver', 'software',
                          'llvm_version', 'null_values',
                          'fn_allocate_varlen_buffer', 'fn_free_buffer')
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
                    x86=32, nvptx=32, arm64=64)[self.arch]

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

    @property
    def llvm_version(self):
        """Return target system LLVM versions tuple.
        """
        return self.info.get('llvm_version')

    @property
    def null_values(self):
        """Return the null values for scalar types serialized as integers.
        """
        null_values = self.info.get('null_values', {})
        if not null_values:
            raise RuntimeError('null value support requires omniscidb-internal PR 5104')
        return null_values

    # TODO: info may also contain: count, threads, cores

    def check_enabled(self, desired_devices):
        """Check if target supports desired devices

        Checks if target is in a set of desired devices. If not, the
        target is considered disabled by the caller.

        Parameters
        ----------
        desired_devices: list
          Device names that is a subset of `['cpu', 'gpu', 'cuda']` or
          None.

        Returns
        -------
        is_contained : bool
          Return True when `desired_devices` is None or when the
          target device name is listed in `desired_devices`.
          Otherwise, return False.
        """
        if desired_devices is None:
            return True
        for d in desired_devices:
            d = d.lower()
            if d == 'cpu':
                if self.is_cpu:
                    return True
            elif d == 'gpu':
                if self.is_gpu:
                    return True
            elif d == 'cuda':
                if self.driver[0] == 'CUDA':
                    return True
            else:
                warnings.warn(
                    f'Ignoring unknown desired device specification: {d}')
        return False

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
        size : {int, None}
          Byte-size of the input type. Dummy target info will not try
          to guess the sizeof of type and will return None.
        """
        s = self.type_sizeof.get(t)
        if s is not None:
            return s
        if isinstance(t, str):
            if t == 'complex':
                return self.sizeof('float') * 2
            if t == 'double':
                return 8  # IEC 60559
            if t == 'float':
                return 4  # IEC 60559
            if t == 'char':
                return 1  # IEC 9899
        if self.name == 'dummy':
            # don't guess
            return
        if isinstance(t, str):
            if t == 'int':
                warnings.warn('Using sizeof(int) == 4')
                return 4  # this is a guess
            if t == 'long':
                warnings.warn('Using sizeof(long) == 8')
                return 8  # this is a guess
            if t == 'longlong':
                warnings.warn('Using sizeof(long long) == 8')
                return 8  # this is a guess
            if t == 'size_t':
                return self.bits // 8
        if isinstance(t, type) and issubclass(t, ctypes._SimpleCData):
            return ctypes.sizeof(t)
        raise NotImplementedError(
            f"{type(self).__name__}[{self.name},{self.triple}].sizeof({t!r})")
