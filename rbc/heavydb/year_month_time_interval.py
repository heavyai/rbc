"""RBC YearMonthTimeInterval type."""
import operator
from rbc import typesystem
from rbc.heavydb import HeavyDBMetaType
from rbc.external import external
from rbc.targetinfo import TargetInfo
from numba.core import extending, cgutils, datamodel
from numba.core import types as nb_types
from .timestamp import TimestampNumbaType, Timestamp

__all__ = [
    "HeavyDBYearMonthTimeIntervalType",
    "YearMonthTimeInterval",
    "YearMonthTimeIntervalNumbaType",
]


daYEAR = 0
daQUARTER = 1
daMONTH = 2
daDAY = 3
daHOUR = 4
daMINUTE = 5
daSECOND = 6
daMILLENNIUM = 7
daCENTURY = 8
daDECADE = 9
daMILLISECOND = 10
daMICROSECOND = 11
daNANOSECOND = 12
daWEEK = 13
daQUARTERDAY = 14
daWEEKDAY = 15
daDAYOFYEAR = 16
daINVALID = 17


class YearMonthTimeInterval(metaclass=HeavyDBMetaType):
    """
        RBC ``YearMonthTimeInterval`` type.

        .. code-block:: c

            struct YearMonthTimeInterval {
                int64_t timeval;
            };

    """

    def __init__(self, timeval: int) -> 'YearMonthTimeInterval':
        pass

    def numStepsBetween(self, start: Timestamp, stop: Timestamp) -> int:
        """
        Return a new  ``Timestamp`` with ``time`` truncated to microseconds
        """
        pass

    def __eq__(self, other: "YearMonthTimeInterval") -> bool:
        """ """
        pass

    def __ne__(self, other: "YearMonthTimeInterval") -> bool:
        """ """
        pass

    def __mul__(self, other: int) -> bool:
        """ """
        pass

    def __add__(self, other: Timestamp) -> bool:
        """ """
        pass


class YearMonthTimeIntervalNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name="YearMonthTimeInterval")


@extending.register_model(YearMonthTimeIntervalNumbaType)
class YearMonthTimeIntervalModel(datamodel.models.StructModel):
    def __init__(self, dmm, fe_type):
        members = [
            ("timeval", nb_types.int64),
        ]
        datamodel.models.StructModel.__init__(self, dmm, fe_type, members)


extending.make_attribute_wrapper(YearMonthTimeIntervalNumbaType, "timeval", "timeval")


class HeavyDBYearMonthTimeIntervalType(typesystem.Type):
    """Typesystem type class for HeavyDB YearMonthTimeInterval structures."""

    @property
    def __typesystem_type__(self):
        return typesystem.Type.fromstring("{int64 timeval}").params(
            name="YearMonthTimeInterval", NumbaType=YearMonthTimeIntervalNumbaType
        )

    def tonumba(self, bool_is_int8=None):
        return YearMonthTimeIntervalNumbaType()

    def postprocess_type(self):
        return self.params(shorttypename='YearMonthTimeInterval')


@extending.type_callable(YearMonthTimeInterval)
def type_heavydb_YearMonthTimeInterval(context):
    def typer(arg):
        if isinstance(arg, nb_types.Integer):
            return typesystem.Type.fromobject("YearMonthTimeInterval").tonumba()

    return typer


@extending.lower_builtin(YearMonthTimeInterval, nb_types.Integer)
def heavydb_YearMonthTimeInterval_int_ctor(context, builder, sig, args):
    timeval = args[0]
    typ = sig.return_type
    dti = cgutils.create_struct_proxy(typ)(context, builder)
    dti.timeval = timeval
    return dti._getvalue()


@extending.overload(operator.mul)
def ol_day_time_interval_operator_mul(a, t):
    if isinstance(a, YearMonthTimeIntervalNumbaType) and isinstance(t, nb_types.Integer):

        def impl(a, t):
            return YearMonthTimeInterval(a.timeval * t)

        return impl


@extending.overload(operator.eq)
def ol_day_time_interval_operator_eq(a, b):
    if isinstance(a, YearMonthTimeIntervalNumbaType) and isinstance(
        b, YearMonthTimeIntervalNumbaType
    ):

        def impl(a, b):
            return a.timeval == b.timeval

        return impl


@extending.overload(operator.ne)
def ol_day_time_interval_operator_ne(a, b):
    if isinstance(a, YearMonthTimeIntervalNumbaType) and isinstance(
        b, YearMonthTimeIntervalNumbaType
    ):

        def impl(a, b):
            return not (a == b)

        return impl


@extending.overload(operator.add)
def ol_day_time_interval_operator_add(a, b):
    from .timestamp import Timestamp, TimestampNumbaType

    target_info = TargetInfo()
    i64_null_val = target_info.null_values["int64"]
    if target_info.is_cpu:
        DateAddHighPrecisionNullable = external(
            "int64 DateAddHighPrecisionNullable(int64, int64, int64, int32, int64)|cpu"
        )

    if isinstance(a, YearMonthTimeIntervalNumbaType) and isinstance(b, TimestampNumbaType):

        def impl(a, b):
            t = DateAddHighPrecisionNullable(
                daMONTH, a.timeval, b.time, 9, i64_null_val
            )
            return Timestamp(t)

        return impl
    elif isinstance(a, TimestampNumbaType) and isinstance(b, YearMonthTimeIntervalNumbaType):

        def impl(a, b):
            t = DateAddHighPrecisionNullable(
                daMONTH, b.timeval, a.time, 9, i64_null_val
            )
            return Timestamp(t)

        return impl


@extending.overload_method(YearMonthTimeIntervalNumbaType, "numStepsBetween")
def ol_day_time_interval_numStepBetween_method(dti, begin, end):

    if isinstance(begin, TimestampNumbaType) and isinstance(end, TimestampNumbaType):

        def impl(dti, begin, end):
            timeval = dti.timeval
            if (timeval > 0 and end.time < begin.time) or (
                timeval < 0 and end.time > begin.time
            ):
                return -1

            first = end.getYear() * 12 + end.getMonth()
            second = begin.getYear() * 12 + begin.getMonth()
            return (first - second) // timeval
        return impl
