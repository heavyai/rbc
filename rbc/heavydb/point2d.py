
'''RBC GeoPoint type that corresponds to HeavyDB type GEOPOINT.
'''

__all__ = ['Point2D']

from .metatype import HeavyDBMetaType
from .abstract_type import HeavyDBAbstractType
from rbc import typesystem
from numba.core import types as nb_types
from numba.core import extending, cgutils
from llvmlite import ir


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


class Point2DNumbaType(nb_types.Type):
    pass


class Point2DNumbaPointerType(nb_types.CPointer):
    pass


class HeavyDBPoint2D(HeavyDBAbstractType):

    @property
    def custom_params(self):
        return {
            'name': 'Point2D',
            'NumbaType': Point2DNumbaType,
            'NumbaPointerType': Point2DNumbaPointerType,
        }

    def tonumba(self, bool_is_int8=None):
        x = typesystem.Type.fromstring('double x')
        y = typesystem.Type.fromstring('double y')
        point2d = typesystem.Type(x, y).params(**self.custom_params)
        return point2d.tonumba(bool_is_int8=True)


@extending.type_callable(Point2D)
def type_heavydb_point2d(context):
    def typer(x, y):
        if isinstance(x, nb_types.Float) and isinstance(y, nb_types.Float):
            return typesystem.Type.fromobject('Point2D').tonumba()
    return typer


@extending.lower_builtin(Point2D, nb_types.Float, nb_types.Float)
def heavydb_timestamp_int_ctor(context, builder, sig, args):
    [x, y] = args
    typ = sig.return_type
    point = cgutils.create_struct_proxy(typ)(context, builder)
    point.x = x
    point.y = y
    return point._getvalue()


@extending.intrinsic
def intrinsic_point2d_get(typingctx, point2d, pos):
    sig = nb_types.double(point2d, pos)

    def codegen(context, builder, sig, args):
        [arg, pos] = args
        assert isinstance(pos, ir.Constant)
        return builder.extract_value(arg, [pos.constant])

    return sig, codegen


@extending.intrinsic
def set_x(typingctx, point, x):
    sig = nb_types.void(point, x)

    def codegen(context, builder, sig, args):
        [p, x] = args
        return builder.insert_value(p, x, [0])


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

