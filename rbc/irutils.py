from rbc.utils import get_version
from numba.core import cgutils, dispatcher, retarget
from llvmlite import ir


int8_t = ir.IntType(8)
int32_t = ir.IntType(32)
int64_t = ir.IntType(64)
void_t = ir.VoidType()
fp32 = ir.FloatType()
fp64 = ir.DoubleType()


def get_or_insert_function(module, fnty, name=None):
    if get_version('numba') >= (0, 54):
        fn = cgutils.get_or_insert_function(
            module, fnty, name=name)
    else:
        fn = module.get_or_insert_function(
            fnty, name=name)
    return fn


def get_member_value(builder, data, idx):
    if get_version('numba') >= (0, 54):
        assert data.opname == 'insertvalue', data.opname
        return builder.extract_value(data, [idx])
    else:
        assert data.opname == 'load', data.opname
        struct = data.operands[0]
        return builder.load(builder.gep(struct, [int32_t(0), int32_t(idx)]))


class Retarget(retarget.BasicRetarget):

    def __init__(self, target_name):
        self.target_name = target_name
        super().__init__()

    @property
    def output_target(self):
        return self.target_name

    def compile_retarget(self, cpu_disp):
        from numba import njit
        kernel = njit(_target=self.target_name)(cpu_disp.py_func)
        return kernel


def switch_target(target_name):
    if get_version('numba') > (0, 55):
        tc = dispatcher.TargetConfigurationStack
    else:
        tc = dispatcher.TargetConfig

    return tc.switch_target(Retarget(target_name))
