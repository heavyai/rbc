import pytest
from rbc.tests import omnisci_fixture


@pytest.fixture(scope='module')
def omnisci():

    for o in omnisci_fixture(globals(), minimal_version=(5, 5)):
        define(o)

        def require_loadtime(kind, _cache=[None]):
            msg = _cache[0]
            if kind == 'lt':
                try:
                    if msg is not None:
                        raise msg
                    if o.has_cuda:
                        o.sql_execute('select lt_device_selection_udf_gpu(0)')
                    else:
                        o.sql_execute('select lt_device_selection_udf_cpu(0)')
                except Exception as msg:
                    _cache[0] = msg
                    pytest.skip(f'test requires load-time device selection UDFs ({msg}):'
                                f' run server with `--udf ../Tests/device_selection_samples.cpp`'
                                ' or check server logs for errors')

        o.require_loadtime = require_loadtime
        yield o


funcs = ('any', 'cpu', 'gpu', 'both')
kinds = ('rt', 'ct', 'lt')

cpu_device_code = 0x637075
gpu_device_code = 0x677075
any_device_code = 0x616e79
err_device_code = 0x657272


def decode(c):
    return {cpu_device_code: 'cpu',
            gpu_device_code: 'gpu',
            any_device_code: 'any',
            err_device_code: 'err'}[c]


def get_single_result(omnisci, kind, func):
    if func == 'any':
        if kind == 'rt':
            return any_device_code
        return gpu_device_code if omnisci.has_cuda else cpu_device_code
    if func == 'cpu':
        return cpu_device_code
    if func == 'gpu':
        return gpu_device_code if omnisci.has_cuda else err_device_code
    if func == 'both':
        return gpu_device_code if omnisci.has_cuda else cpu_device_code
    raise NotImplementedError(repr((kind, func)))


def get_pair_result(omnisci, kind1, func1, kind2, func2):
    r1 = get_single_result(omnisci, kind1, func1)
    r2 = get_single_result(omnisci, kind2, func2)
    if r1 == err_device_code or r2 == err_device_code:
        return err_device_code
    if func1 == 'cpu' and func2 == 'gpu':
        return err_device_code
    if func1 == 'gpu' and func2 == 'cpu':
        return err_device_code
    if r1 == cpu_device_code or r2 == cpu_device_code:
        if any_device_code not in [r1, r2]:
            return cpu_device_code, cpu_device_code
    return r1, r2


def execute1(omnisci, query):
    try:
        _, result = omnisci.sql_execute(query)
    except Exception as msg:
        return msg
    else:
        result = list(result)
        return decode(result[0][0])


def execute2(omnisci, query):
    try:
        _, result = omnisci.sql_execute(query)
    except Exception as msg:
        return msg
    else:
        result = list(result)
        return decode(result[0][0]), decode(result[0][1])


def get_worker1(omnisci, ext, kind, func):
    expected = decode(get_single_result(omnisci, kind, func))
    if ext == 'udf':
        query = f'select {kind}_device_selection_{ext}_{func}(0)'
        return execute1, query, expected
    if ext == 'udtf':
        query = (f'select out0 from table({kind}_device_selection_{ext}_{func}'
                 f'(cursor(select i4 from {omnisci.table_name})))')
        return execute1, query, expected
    raise NotImplementedError(repr((ext, kind, func)))


def get_worker2(omnisci, ext, kind1, func1, kind2, func2):
    expected = get_pair_result(omnisci, kind1, func1, kind2, func2)
    if isinstance(expected, tuple):
        expected = tuple(map(decode, expected))
    else:
        expected = decode(expected)
    if ext == 'udf/udf':
        query = (f'select {kind1}_device_selection_udf_{func1}(0),'
                 f' {kind2}_device_selection_udf_{func2}(0)')
        return execute2, query, expected
    if ext == 'udtf/udf':
        query = (f'select out0 from table({kind1}_device_selection_udtf_{func1}'
                 f'(cursor(select {kind2}_device_selection_udf_{func2}(i4)'
                 f' from {omnisci.table_name})))')
        return execute1, query, expected[0] if isinstance(expected, tuple) else expected
    raise NotImplementedError(repr((ext, kind1, func1, kind2, func2)))


def define(omnisci):

    @omnisci('int32(int32)')
    def rt_device_selection_udf_any(x):
        # cannot determine which device is actually used
        return any_device_code

    @omnisci('int32(int32)', devices=['cpu'])
    def rt_device_selection_udf_cpu(x):
        return cpu_device_code

    @omnisci('int32(int32)', devices=['gpu'])
    def rt_device_selection_udf_gpu(x):
        return gpu_device_code

    @omnisci('int32(int32)', devices=['cpu'])  # NOQA
    def rt_device_selection_udf_both(x):  # NOQA
        return cpu_device_code

    @omnisci('int32(int32)', devices=['gpu'])  # NOQA
    def rt_device_selection_udf_both(x):  # NOQA
        return gpu_device_code

    @omnisci('int32(Column<int32>, OutputColumn<int32>)')
    def rt_device_selection_udtf_any(x, out):
        # cannot determine which device is actually used
        out[0] = any_device_code
        return 1

    @omnisci('int32(Column<int32>, OutputColumn<int32>)', devices=['cpu'])
    def rt_device_selection_udtf_cpu(x, out):
        out[0] = cpu_device_code
        return 1

    @omnisci('int32(Column<int32>, OutputColumn<int32>)', devices=['gpu'])
    def rt_device_selection_udtf_gpu(x, out):
        out[0] = gpu_device_code
        return 1

    @omnisci('int32(Column<int32>, OutputColumn<int32>)', devices=['cpu'])  # NOQA
    def rt_device_selection_udtf_both(x, out):  # NOQA
        out[0] = cpu_device_code
        return 1

    @omnisci('int32(Column<int32>, OutputColumn<int32>)', devices=['gpu'])  # NOQA
    def rt_device_selection_udtf_both(x, out):  # NOQA
        out[0] = gpu_device_code
        return 1


@pytest.mark.parametrize("func", funcs)
@pytest.mark.parametrize("ext", ['udf', 'udtf'])
@pytest.mark.parametrize("kind", ['rt', 'ct', 'lt'])
def test_device_selection_single(omnisci, func, ext, kind):
    omnisci.require_version((5, 5), 'omniscidb-internal PR 5026')
    omnisci.require_loadtime(kind)

    if kind == 'lt' and ext == 'udtf':
        pytest.skip('Load-time UDTFs not supported')

    execute, query, expected = get_worker1(omnisci, ext, kind, func)
    result = execute(omnisci, query)
    if isinstance(result, Exception):
        assert expected == decode(err_device_code), str(result)
    else:
        assert expected == result


@pytest.mark.parametrize("func12", ['any/any', 'any/cpu', 'any/gpu', 'any/both',
                                    'both/cpu', 'both/gpu', 'cpu/gpu', 'cpu/cpu',
                                    'gpu/gpu', 'both/both'])
@pytest.mark.parametrize("ext", ['udf/udf', 'udtf/udf'])
@pytest.mark.parametrize("kind2", kinds)
@pytest.mark.parametrize("kind1", kinds)
def test_device_selection_pair(omnisci, func12, ext, kind2, kind1):
    omnisci.require_version((5, 5), 'omniscidb-internal PR 5026')
    func12 = tuple(func12.split('/'))
    func1, func2 = func12

    omnisci.require_loadtime(kind1)
    omnisci.require_loadtime(kind2)
    if 'udtf' in ext and kind1 == 'lt':
        pytest.skip('Load-time UDTFs not supported')

    execute, query, expected = get_worker2(omnisci, ext, kind1, func1, kind2, func2)
    result = execute(omnisci, query)
    if isinstance(result, Exception):
        assert expected == decode(err_device_code), str(result)
    else:
        assert expected == result
