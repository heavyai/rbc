import pytest
from rbc.stdlib import array_api
from rbc.tests import heavydb_fixture
from numba import TypingError


@pytest.fixture(scope="module")
def heavydb():

    for o in heavydb_fixture(globals()):
        yield o


unsupported_functions = [
    # statistical_function
    'std', 'var',
    # manipulation_functions
    "concat", "expand_dims", "flip", "permute_dims", "reshape", "roll", "squeeze", "stack",
    # utility_functions
    "all", "any",
    # sorting_functions
    "argsort", "sort",
    # searching_functions
    "argmax", "argmin", "nonzero", "where",
    # elementwise_functions
    'float_power', 'divmod', 'cbrt', 'isnat',
    # set_functions
    "unique_all", "unique_counts", "unique_inverse", "unique_values",
    # linear_algebra_functions
    "matmul", "matrix_transpose", "tensordot", "vecdot",
    # data_type_functions
    "astype", "broadcast_arrays", "broadcast_to", "can_cast", "finfo", "iinfo", "result_type",
    # creation_functions
    'arange', 'asarray', 'eye', 'from_dlpack', 'linspace', 'meshgrid', 'tril', 'triu',
]


# ensure unimplemented functions raise a meaninful exception
@pytest.mark.parametrize('func_name', unsupported_functions)
def test_unimplemented(heavydb, func_name):

    func = getattr(array_api, func_name)

    @heavydb('int64(int64)')
    def test_exception_raised(x):
        return func(x)

    # NumbaNotSupportedError is captured and a TypingError is returned instead
    with pytest.raises(TypingError, match=f'Function "{func_name}" is not supported.'):
        heavydb.register()
    heavydb.unregister()
