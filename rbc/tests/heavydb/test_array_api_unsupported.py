import pytest
from numba import TypingError

from rbc.stdlib import array_api
from rbc.tests import heavydb_fixture


@pytest.fixture(scope="module")
def heavydb():

    for o in heavydb_fixture(globals(), load_test_data=False):
        yield o


unsupported_functions = [
    # manipulation_functions
    "expand_dims", "permute_dims", "reshape", "roll", "squeeze", "stack",
    # sorting_functions
    "argsort", "sort",
    # elementwise_functions
    'float_power', 'divmod', 'cbrt', 'isnat',
    # set_functions
    "unique_all", "unique_counts", "unique_inverse",
    # linear_algebra_functions
    "matmul", "matrix_transpose", "tensordot", "vecdot",
    # data_type_functions
    "broadcast_arrays", "broadcast_to", "isdtype",
    # creation_functions
    'eye', 'from_dlpack', 'linspace', 'meshgrid', 'tril', 'triu',
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
