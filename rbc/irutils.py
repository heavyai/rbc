from rbc.utils import get_version
from numba.core import cgutils
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
