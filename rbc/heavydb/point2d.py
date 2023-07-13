
'''RBC Point2D type.
'''

__all__ = ['Point2D']

from llvmlite import ir
from numba.core import cgutils, extending
from numba.core import types as nb_types

from rbc import typesystem

from .metatype import HeavyDBMetaType

i32 = ir.IntType(32)
i64 = ir.IntType(64)


class Point2D(metaclass=HeavyDBMetaType):
    """

    A Point2D struct is just a wrapper for a pair of doubles

    ... code-block:: c

        {
            double x;
            double y;
        }
    """

    def __init__(self, x: float, y: float) -> None:
        pass

    @property
    def x(self) -> float:
        pass

    @property
    def y(self) -> float:
        pass


class Point2DNumbaType(nb_types.Type):
    pass


class HeavyDBPoint2D(typesystem.Type):

    @property
    def custom_params(self):
        return {
            'name': 'Point2D',
            'NumbaType': Point2DNumbaType,
        }

    def tonumba(self, bool_is_int8=None):
        x = typesystem.Type.fromstring('double x')
        y = typesystem.Type.fromstring('double y')
        point2d = typesystem.Type(x, y).params(**self.custom_params)
        return point2d.tonumba(bool_is_int8=True)


@extending.type_callable(Point2D)
def type_heavydb_point2d(context):
    def typer(x, y):
        if isinstance(x, nb_types.Number) and isinstance(y, nb_types.Number):
            return typesystem.Type.fromobject('Point2D').tonumba()
    return typer


@extending.lower_builtin(Point2D, nb_types.Number, nb_types.Number)
def heavydb_point2d_ctor(context, builder, sig, args):
    [x, y] = args
    typ = sig.return_type
    point = cgutils.create_struct_proxy(typ)(context, builder)
    point.x = context.cast(builder, x, sig.args[0], nb_types.double)
    point.y = context.cast(builder, y, sig.args[1], nb_types.double)
    return point._getvalue()


@extending.intrinsic
def intrinsic_point2d_get(typingctx, point2d, pos):
    sig = nb_types.double(point2d, pos)

    def codegen(context, builder, sig, args):
        [arg, pos] = args
        assert isinstance(pos, ir.Constant)
        return builder.extract_value(arg, [pos.constant])

    return sig, codegen


@extending.overload_attribute(Point2DNumbaType, 'x')
def impl_poind2d_attr_x(point2d):
    def impl(point2d):
        return intrinsic_point2d_get(point2d, 0)
    return impl


@extending.overload_attribute(Point2DNumbaType, 'y')
def impl_poind2d_attr_y(point2d):
    def impl(point2d):
        return intrinsic_point2d_get(point2d, 1)
    return impl
