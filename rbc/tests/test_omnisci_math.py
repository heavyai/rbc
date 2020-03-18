import pytest


rbc_omnisci = pytest.importorskip('rbc.omniscidb')
np = pytest.importorskip('numpy')
nb = pytest.importorskip('numba')
available_version, reason = rbc_omnisci.is_available()
pytestmark = pytest.mark.skipif(not available_version, reason=reason)


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
        x = 0.7 + _i/10.0
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


numpy_functions = [
    # Arithmetic functions:
    ('absolute', 'double(double)', np.absolute),
    ('conjugate', 'double(double)', np.conjugate),
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
def test_numpy_function(omnisci, fn_name, signature, np_func):
    omnisci.reset()

    arity = signature.count(',') + 1
    kind = signature.split('(')[1].split(',')[0].split(')')[0]
    if isinstance(np_func, np.ufunc):
        # numba does not support jitting ufunc-s directly
        if arity == 1:
            np_func = eval(f'lambda x: np.{np_func.__name__}(x)', dict(np=np))
        elif arity == 2:
            np_func = eval(f'lambda x, y: np.{np_func.__name__}(x, y)',
                           dict(np=np))
        else:
            raise NotImplementedError((signature, arity))
    if np_func.__name__ == '<lambda>':
        # give lambda function a name
        np_func.__name__ = fn_name

    if fn_name in ['logical_or', 'logical_xor', 'logical_and', 'logical_not']:
        # Invalid use of Function(<ufunc 'logical_or'>) with
        # argument(s) of type(s): (boolean8, boolean8)
        pytest.skip(f'using boolean8 as {fn_name} argument not implemented')

    if fn_name in ['positive', 'float_power', 'cbrt', 'divmod0', 'heaviside',
                   'frexp0']:
        try:
            if arity == 1:
                nb.njit(np_func)(0.5)
            elif arity == 2:
                nb.njit(np_func)(0.5, 0.5)
        except nb.errors.TypingError as msg:
            msg = str(msg).splitlines()[1]
            pytest.skip(msg)

    if fn_name in ['ldexp', 'spacing', 'nextafter', 'signbit']:
        pytest.skip(f'{fn_name}: FIXME')

    if omnisci.version < (5, 2) and fn_name in [
            'sinh', 'cosh', 'tanh', 'rint', 'trunc', 'expm1', 'exp2', 'log2',
            'log1p', 'gcd', 'lcm', 'around', 'fmod', 'hypot']:
        # fix forbidden names
        fn_name += 'FIX'
        np_func.__name__ = fn_name

    omnisci(signature)(np_func)

    omnisci.register()

    if kind == 'double':
        assert arity <= 3, arity
        xs = ', '.join('xyz'[:arity])
    elif kind.startswith('int'):
        assert arity <= 2, arity
        xs = ', '.join('ij'[:arity])
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
