import math
import pytest
import sys
import rbc.omnisci_backend as omni  # noqa: F401

rbc_omnisci = pytest.importorskip('rbc.omniscidb')
np = pytest.importorskip('numpy')
nb = pytest.importorskip('numba')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


@pytest.fixture(scope='module')
def nb_version():
    from rbc.utils import get_version
    return get_version('numba')


@pytest.fixture(scope='module')
def omnisci():
    config = rbc_omnisci.get_client_config(debug=not True)
    m = rbc_omnisci.RemoteOmnisci(**config)
    table_name = 'rbc_test_omnisci_math'
    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))

    m.sql_execute(
        'CREATE TABLE IF NOT EXISTS {table_name}'
        ' (a BOOLEAN, b BOOLEAN, x DOUBLE, y DOUBLE, z DOUBLE, i INT, '
        'j INT, t INT[], td DOUBLE[], te INT[]);'
        .format(**locals()))

    for _i in range(1, 6):
        a = str((_i % 3) == 0).lower()
        b = str((_i % 2) == 0).lower()
        x = 0.1 + _i/10.0
        y = _i/6.0
        z = _i + 1.23
        i = _i
        j = i * 10
        t = 'ARRAY[%s]' % (', '.join(str(j + i) for i in range(-i, i+1)))
        td = 'ARRAY[%s]' % (', '.join(str(j + i/1.0) for i in range(-i, i+1)))
        te = 'Array[]'
        m.sql_execute(
            'insert into {table_name} values (\'{a}\', \'{b}\', {x}, {y},'
            ' {z}, {i}, {j}, {t}, {td}, {te})'
            .format(**locals()))

    m.table_name = table_name
    yield m

    m.sql_execute('DROP TABLE IF EXISTS {table_name}'.format(**locals()))


math_functions = [
    # Number-theoretic and representation functions
    ('ceil', 'int64(double)'),
    ('comb', 'int64(int64, int64)'),
    ('copysign', 'double(double, double)'),
    ('fabs', 'double(double)'),
    ('factorial', 'int64(int64)'),
    ('floor', 'double(double)'),
    ('fmod', 'double(double, double)'),
    ('frexp', 'double(double)'),  # returns a pair (m, e)
    ('fsum', 'double(double[])'),
    ('gcd', 'int(int, int)'),
    ('isclose', 'bool(double, double)'),
    ('isfinite', 'bool(double)'),
    ('isinf', 'bool(double)'),
    ('isnan', 'bool(double)'),
    ('isqrt', 'int64(int64)'),
    ('ldexp', 'double(double, int)'),
    ('modf', 'double(double, double)'),
    ('perm', 'int(int, int)'),
    ('prod', 'int64(int64[])'),
    ('remainder', 'double(double, double)'),
    ('trunc', 'double(double)'),
    # Power and logarithmic functions
    ('exp', 'double(double)'),
    ('expm1', 'double(double)'),
    ('log', 'double(double)'),
    ('log1p', 'double(double)'),
    ('log2', 'double(double)'),
    ('log10', 'double(double)'),
    ('pow', 'double(double, double)'),
    ('sqrt', 'double(double)'),
    # # Trigonometric functions
    ('acos', 'double(double)'),
    ('asin', 'double(double)'),
    ('atan', 'double(double)'),
    ('atan2', 'double(double, double)'),
    ('cos', 'double(double)'),
    ('dist', 'int64(int64[], int64[])'),
    ('hypot', 'double(double, double)'),
    ('sin', 'double(double)'),
    ('tan', 'double(double)'),
    ('degrees', 'double(double)'),
    ('radians', 'double(double)'),
    # # Hyperbolic functions
    ('acosh', 'double(double)'),
    ('asinh', 'double(double)'),
    ('atanh', 'double(double)'),
    ('cosh', 'double(double)'),
    ('sinh', 'double(double)'),
    ('tanh', 'double(double)'),
    # # Special functions
    ('erf', 'double(double)'),
    ('erfc', 'double(double)'),
    ('gamma', 'double(double)'),
    ('lgamma', 'double(double)'),
    # Constants
    ('pi', 'double(double)'),
    ('e', 'double(double)'),
    ('tau', 'double(double)'),
    ('inf', 'double(double)'),
    ('nan', 'double(double)'),
]


@pytest.mark.parametrize("fn_name, signature", math_functions,
                         ids=[item[0] for item in math_functions])
def test_math_function(omnisci, nb_version, fn_name, signature):
    omnisci.reset()

    math_func = getattr(math, fn_name, None)
    if math_func is None:
        pytest.skip(f'{fn_name}: not available in {math.__name__} module'
                    f' of Python {sys.version.split(None, 1)[0]}')

    if fn_name in ['prod', 'remainder', 'log2', 'comb', 'factorial', 'fsum',
                   'fmod', 'isclose', 'isqrt', 'ldexp', 'modf', 'dist',
                   'perm']:
        pytest.skip(f'{fn_name}: Numba uses cpython implementation! [rbc issue 156]')

    if omnisci.version < (5, 5) and omnisci.has_cuda and \
        fn_name in ['gcd', 'comb', 'factorial', 'fsum', 'isclose', 'isfinite',
                    'isqrt', 'ldexp', 'modf', 'perm', 'prod', 'remainder', 'log2',
                    'trunc', 'dist', 'fmod']:
        pytest.skip(f'CUDA target does not support {fn_name} function [rbc issue 156]')

    if omnisci.version < (5, 5) and omnisci.has_cuda and fn_name in ['floor']:
        pytest.skip(f'{fn_name} compilation crashes due to typing differences with CPU target'
                    ' [rbc issue 203]')

    if omnisci.version < (5, 5) and omnisci.has_cuda and fn_name in ['pow', 'gamma', 'lgamma']:
        pytest.skip(f'{fn_name} crashes with CUDA-enabled server [rbc issue 156/158]')

    if fn_name in ['frexp']:
        pytest.skip(f'{fn_name} returns a pair (m, e) [rbc issue 156/202]')

    if omnisci.version < (5, 5) and omnisci.has_cuda and nb_version < (0, 52):
        if fn_name in ['expm1', 'log1p', 'hypot', 'acosh', 'asinh', 'atanh',
                       'cosh', 'sinh', 'tanh', 'erf', 'erfc', 'acos', 'asin',
                       'atan', 'atan2']:
            pytest.skip(f'{fn_name} requires numba version 0.52, currently using'
                        f' {".".join(map(str, nb_version))}')

    arity = signature.count(',') + 1
    kind = signature.split('(')[1].split(',')[0].split(')')[0]

    if fn_name in ['pi', 'e', 'tau', 'inf', 'nan']:
        fn = eval(f'lambda x: math.{fn_name}', dict(math=math))
    elif arity == 1:
        fn = eval(f'lambda x: math.{fn_name}(x)', dict(math=math))
    elif arity == 2:
        fn = eval(f'lambda x, y: math.{fn_name}(x, y)',
                  dict(math=math))
    else:
        raise NotImplementedError((signature, arity))

    fprefix = 'rbc_test_'  # to avoid conflicts with SQL functions

    if fn.__name__ == '<lambda>':
        # give lambda function a name
        fn.__name__ = fn_name

    fn.__name__ = fprefix + fn.__name__

    omnisci(signature)(fn)

    omnisci.register()

    if kind == 'double':
        assert arity <= 3, arity
        xs = ', '.join('xyz'[:arity])
    elif kind.startswith('int'):
        assert arity <= 2, arity
        xs = ', '.join('ij'[:arity])
    elif kind.startswith('bool'):
        assert arity <= 2, arity
        xs = ', '.join('ab'[:arity])
    elif kind == 'constant':
        xs = ''
    else:
        raise NotImplementedError(kind)

    if fn_name in ['acosh', 'asinh']:
        xs = 'z'

    if fn_name in ['ldexp']:
        xs = 'x, i'

    query = f'select {xs}, {fprefix}{fn_name}({xs}) from {omnisci.table_name}'
    descr, result = omnisci.sql_execute(query)
    for args in list(result):
        result = args[-1]
        if fn_name in ['pi', 'e', 'tau', 'inf', 'nan']:
            expected = math_func
        else:
            expected = math_func(*args[:-1])
        if np.isnan(expected):
            assert np.isnan(result)
        else:
            assert(np.isclose(expected, result))


numpy_functions = [
    # Arithmetic functions:
    ('absolute', 'double(double)', np.absolute),
    ('conjugate', 'double(double)', np.conjugate),
    ('conj', 'double(double)', np.conjugate),
    ('fabs', 'double(double)', np.fabs),
    ('fmax', 'double(double, double)', np.fmax),
    ('fmin', 'double(double, double)', np.fmin),
    ('maximum', 'double(double, double)', np.maximum),
    ('minimum', 'double(double, double)', np.minimum),
    ('positive', 'double(double)', np.positive),
    ('negative', 'double(double)', np.negative),
    ('sign', 'double(double)', np.sign),
    ('reciprocal', 'double(double)', np.reciprocal),
    ('add', 'double(double, double)', np.add),
    ('subtract', 'double(double, double)', np.subtract),
    ('multiply', 'double(double, double)', np.multiply),
    ('divide', 'double(double, double)', np.divide),
    ('true_divide', 'double(double, double)', np.true_divide),
    ('floor_divide', 'double(double, double)', np.floor_divide),
    ('power', 'double(double, double)', np.power),
    ('float_power', 'double(double, double)', np.float_power),
    ('square', 'double(double)', np.square),
    ('sqrt', 'double(double)', np.sqrt),
    ('cbrt', 'double(double)', np.cbrt),   # not supported by numba
    ('remainder', 'double(double, double)', np.remainder),
    ('fmod', 'double(double, double)', np.fmod),
    ('modf', 'double(double, double)', np.mod),
    ('modi', 'int(int, int)', np.mod),
    ('divmod0', 'int(int, int)', lambda i, j: np.divmod(i, j)[0]),
    # Trigonometric functions:
    ('sin', 'double(double)', np.sin),
    ('cos', 'double(double)', np.cos),
    ('tan', 'double(double)', np.tan),
    ('arcsin', 'double(double)', np.arcsin),
    ('arccos', 'double(double)', np.arccos),
    ('arctan', 'double(double)', np.arctan),
    ('arctan2', 'double(double, double)', np.arctan2),
    ('hypot', 'double(double, double)', np.hypot),
    ('radians', 'double(double)', np.radians),
    ('rad2deg', 'double(double)', np.rad2deg),
    ('deg2rad', 'double(double)', np.deg2rad),
    ('degrees', 'double(double)', np.degrees),
    # Hyperbolic functions:
    ('sinh', 'double(double)', np.sinh),
    ('cosh', 'double(double)', np.cosh),
    ('tanh', 'double(double)', np.tanh),
    ('arcsinh', 'double(double)', np.arcsinh),
    ('arccosh', 'double(double)', np.arccosh),
    ('arctanh', 'double(double)', np.arctanh),
    # Exp-log functions:
    ('exp', 'double(double)', np.exp),
    ('expm1', 'double(double)', np.expm1),
    ('exp2', 'double(double)', np.exp2),
    ('log', 'double(double)', np.log),
    ('log10', 'double(double)', np.log10),
    ('log2', 'double(double)', np.log2),
    ('log1p', 'double(double)', np.log1p),
    ('logaddexp', 'double(double, double)', np.logaddexp),
    ('logaddexp2', 'double(double, double)', np.logaddexp2),
    ('ldexp', 'double(double, int)', np.ldexp),
    ('frexp0', 'double(double)', lambda x: np.frexp(x)[0]),
    # Rounding functions:
    ('around', 'double(double)', lambda x: np.around(x)),
    ('round2',  # round and round_ are not good names
     'double(double)', lambda x: np.round_(x)),  # force arity to 1
    ('floor', 'double(double)', np.floor),
    ('ceil', 'double(double)', np.ceil),
    ('trunc', 'double(double)', np.trunc),
    ('rint', 'double(double)', np.rint),
    ('spacing', 'double(double)', np.spacing),
    ('nextafter', 'double(double, double)', np.nextafter),
    # Rational functions:
    ('gcd', 'int(int, int)', np.gcd),
    ('lcm', 'int(int, int)', np.lcm),
    ('right_shift', 'int(int, int)', np.right_shift),
    ('left_shift', 'int64(int64, int64)', np.left_shift),
    # Misc functions
    ('heaviside', 'double(double, double)', np.heaviside),
    ('copysign', 'double(double, double)', np.copysign),
    # Bit functions:
    ('invert', 'int(int)', np.invert),
    ('bitwise_not', 'int(int)', np.bitwise_not),
    ('bitwise_or', 'int(int, int)', np.bitwise_or),
    ('bitwise_xor', 'int(int, int)', np.bitwise_xor),
    ('bitwise_and', 'int(int, int)', np.bitwise_and),
    # Logical functions:
    ('isfinite', 'bool(double)', np.isfinite),
    ('isinf', 'bool(double)', np.isinf),
    ('isnan', 'bool(double)', np.isnan),
    ('signbit', 'bool(double)', np.signbit),
    ('less', 'bool(double, double)', np.less),
    ('less_equal', 'bool(double, double)', np.less_equal),
    ('greater', 'bool(double, double)', np.greater),
    ('greater_equal', 'bool(double, double)', np.greater_equal),
    ('equal', 'bool(double, double)', np.equal),
    ('not_equal', 'bool(double, double)', np.not_equal),
    ('logical_or', 'bool(bool, bool)', np.logical_or),
    ('logical_xor', 'bool(bool, bool)', np.logical_xor),
    ('logical_and', 'bool(bool, bool)', np.logical_and),
    ('logical_not', 'bool(bool)', np.logical_not),
    # missing ufunc-s as unsupported: matmul, isnat
]

if np is not None:
    for n, f in np.__dict__.items():
        if n in ['matmul', 'isnat']:  # UNSUPPORTED
            continue
        if isinstance(f, np.ufunc):
            for item in numpy_functions:
                if item[0].startswith(f.__name__):
                    break
            else:
                print(f'TODO: ADD {n} TEST TO {__file__}')


@pytest.mark.parametrize("fn_name, signature, np_func", numpy_functions,
                         ids=[item[0] for item in numpy_functions])
def test_numpy_function(omnisci, nb_version, fn_name, signature, np_func):
    omnisci.reset()

    if fn_name in ['cbrt', 'float_power']:
        pytest.skip(f'Numba does not support {fn_name}')

    arity = signature.count(',') + 1
    kind = signature.split('(')[1].split(',')[0].split(')')[0]
    if isinstance(np_func, np.ufunc):
        # numba does not support jitting ufunc-s directly
        if arity == 1:
            fn = eval(f'lambda x: omni.{np_func.__name__}(x)', dict(omni=omni))
        elif arity == 2:
            fn = eval(f'lambda x, y: omni.{np_func.__name__}(x, y)',
                      dict(omni=omni))
        else:
            raise NotImplementedError((signature, arity))
    else:
        fn = np_func

    if fn.__name__ == '<lambda>':
        # give lambda function a name
        fn.__name__ = fn_name

    if available_version[:2] < (5, 4) and fn_name in \
            ['logical_or', 'logical_xor', 'logical_and', 'logical_not']:
        pytest.skip(
            f"using boolean arguments requires omniscidb v 5.4 or newer"
            f" (got {available_version}) [issue 108]")

    if fn_name in ['positive', 'divmod0', 'frexp0']:
        try:
            if arity == 1:
                nb.njit(fn)(0.5)
            elif arity == 2:
                nb.njit(fn)(0.5, 0.5)
        except nb.errors.TypingError as msg:
            msg = str(msg).splitlines()[1]
            pytest.skip(msg)

    if fn_name in ['spacing']:
        # Skipping spacing__cpu_0 that uses undefined function `npy_spacing`
        pytest.skip(f'{fn_name}: FIXME')

    if omnisci.version < (5, 5) and omnisci.has_cuda and fn_name in ['lcm']:
        # https://github.com/xnd-project/rbc/issues/71
        pytest.skip(f'{fn_name}: crashes CUDA enabled omniscidb server'
                    ' [rbc issue 71]')

    if omnisci.version < (5, 2) and nb_version < (0, 52) and omnisci.has_cuda and fn_name in [
            'arcsin', 'arccos', 'arctan', 'arctan2', 'hypot', 'sinh', 'cosh',
            'tanh', 'arcsinh', 'arccosh', 'arctanh', 'expm1', 'exp2', 'log2',
            'log1p', 'logaddexp2', 'ldexp', 'lcm', 'logaddexp', 'nextafter']:
        pytest.skip(f"{fn_name}: libdevice bindings requires numba 0.52 or newer,"
                    f" got Numba v{'.'.join(map(str, nb_version))}")

    if omnisci.version < (5, 2) and fn_name in [
            'sinh', 'cosh', 'tanh', 'rint', 'trunc', 'expm1', 'exp2', 'log2',
            'log1p', 'gcd', 'lcm', 'around', 'fmod', 'hypot']:
        # fix forbidden names
        fn_name += 'FIX'
        fn.__name__ = fn_name

    if omnisci.version < (5, 2) and omnisci.has_cuda and fn_name in [
            'fmax', 'fmin', 'power', 'sqrt', 'tan', 'radians', 'degrees'
    ]:
        # NativeCodegen.cpp:849 use of undefined value '@llvm.maxnum.f64'
        # NativeCodegen.cpp:849 invalid redefinition of function 'power'
        # NativeCodegen.cpp:849 invalid redefinition of function
        #                       'llvm.lifetime.start.p0i8'
        # NativeCodegen.cpp:849 invalid redefinition of function 'radians'
        pytest.skip(f'{fn_name}: crashes CUDA enabled omniscidb server < 5.2')

    omnisci(signature)(fn)

    omnisci.register()

    if fn_name == 'ldexp':
        xs = ', '.join('xi')
    elif kind == 'double':
        assert arity <= 3, arity
        xs = ', '.join('xyz'[:arity])
    elif kind.startswith('int'):
        assert arity <= 2, arity
        xs = ', '.join('ij'[:arity])
    elif kind.startswith('bool'):
        assert arity <= 2, arity
        xs = ', '.join('ab'[:arity])
    else:
        raise NotImplementedError(kind)

    query = f'select {xs}, {fn_name}({xs}) from {omnisci.table_name}'
    descr, result = omnisci.sql_execute(query)
    for args in list(result):
        result = args[-1]
        expected = np_func(*args[:-1])
        if np.isnan(expected):
            assert np.isnan(result)
        else:
            assert(np.isclose(expected, result))
