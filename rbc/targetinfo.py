import ctypes


class TargetInfo(object):
    """Base class for determining various information about the target
    system.
    """

    def __init__(self, name='cpu', strict=False):
        """
        Parameters
        ----------
        name : str
          The name of a target device. Typically 'cpu' or 'gpu'.
        strict: bool
          When True, require that atomic types are concrete. If not,
          raise an exception.
        """
        self.name = name
        self.strict = strict
        self.custom_type_converters = []
        self.info = {}

    def set(self, prop, value):
        supported_keys = ('name', 'triple', 'datalayout', 'features',
                          'compute_capability', 'count', 'threads', 'cores')
        if prop not in supported_keys:
            print(f'rbc.{type(self).__name__}:'
                  f' unsupported property {prop}={value}.')
        self.info[prop] = value

    @property
    def triple(self):
        return self.info['triple']

    @property
    def arch(self):
        return self.triple.split('-', 1)[0]

    @property
    def bits(self):
        # expand this dict as needed
        return dict(x86_64=64, nvptx64=64, nvptx=32)[self.arch]

    @property
    def datalayout(self):
        layout = self.info.get('datalayout')
        if layout is None:
            if self.name != 'cpu':
                print(f'rbc.{type(self).__name__}:'
                      f' no datalayout info for {self.name!r} device')
            # In the following we assume that datalayout of the target
            # host matches with the datalayout of the client host,
            # regardless of what OS these hosts run.
            layout = ''
        return layout

    @property
    def device_features(self):
        if 'features' in self.info:
            return ','.join(self.info['features'].split())
        return ''

    @property
    def gpu_cc(self):
        return tuple(map(int, self.info['compute_capability'].split('.')))

    @property
    def device_name(self):
        return self.info['name']

    # info may also contain: count, threads, cores

    def sizeof(self, t):
        """Return the sizeof(t) value in the target system.

        Parameters
        ----------
        t : {str, ...}
            Specify types name. For a full support, one should
            implement the sizeof for the following type names: char,
            uchar, schar, byte, ubyte, short, ushort, int, uint, long,
            ulong, longlong, ulonglong, float, double, longdouble,
            complex, bool, size_t, ssize_t. wchar
        """
        raise NotImplementedError("%s.sizeof(%r)"
                                  % (type(self).__name__, t))

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


class LocalTargetInfo(TargetInfo):
    """Uses ctypes to determine the type info in a local system.

    """

    def sizeof(self, t):
        if isinstance(t, str):
            if t == 'complex':
                return 2 * self.sizeof('float')
            ct = dict(
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
            ).get(t)
            if ct is not None:
                return ctypes.sizeof(ct)
        return super(LocalTargetInfo, self).sizeof(t)
