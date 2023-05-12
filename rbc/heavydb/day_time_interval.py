"""RBC DayTimeInterval type."""
import operator
from rbc import typesystem
from rbc.heavydb import HeavyDBMetaType
from rbc.external import external
from rbc.targetinfo import TargetInfo
from rbc.errors import UnsupportedError
from numba.core import extending, cgutils, datamodel
from numba.core import types as nb_types
from .timestamp import TimestampNumbaType, Timestamp, kMicroSecsPerSec

__all__ = [
    "HeavyDBDayTimeIntervalType",
    "DayTimeInterval",
    "DayTimeIntervalNumbaType",
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


class DayTimeInterval(metaclass=HeavyDBMetaType):
    """
        RBC ``DayTimeInterval`` type.

        .. code-block:: c

            struct DayTimeInterval {
                int64_t timeval;
            };

    """

    def __init__(self, timeval: int) -> 'DayTimeInterval':
        pass

    def numStepsBetween(self, start: Timestamp, stop: Timestamp) -> int:
        """
        Return a new  ``Timestamp`` with ``time`` truncated to microseconds
        """
        pass

    def __eq__(self, other: "DayTimeInterval") -> bool:
        """ """
        pass

    def __ne__(self, other: "DayTimeInterval") -> bool:
        """ """
        pass

    def __mul__(self, other: int) -> bool:
        """ """
        pass

    def __add__(self, other: Timestamp) -> bool:
        """ """
        pass


class DayTimeIntervalNumbaType(nb_types.Type):
    def __init__(self):
        super().__init__(name="DayTimeInterval")


@extending.register_model(DayTimeIntervalNumbaType)
class DayTimeIntervalModel(datamodel.models.StructModel):
    def __init__(self, dmm, fe_type):
        members = [
            ("timeval", nb_types.int64),
        ]
        datamodel.models.StructModel.__init__(self, dmm, fe_type, members)


extending.make_attribute_wrapper(DayTimeIntervalNumbaType, "timeval", "timeval")


class HeavyDBDayTimeIntervalType(typesystem.Type):
    """Typesystem type class for HeavyDB DayTimeInterval structures."""

    @property
    def __typesystem_type__(self):
        return typesystem.Type.fromstring("{int64 timeval}").params(
            name="DayTimeInterval", NumbaType=DayTimeIntervalNumbaType
        )

    def tonumba(self, bool_is_int8=None):
        return DayTimeIntervalNumbaType()

    def postprocess_type(self):
        return self.params(shorttypename='DayTimeInterval')


@extending.type_callable(DayTimeInterval)
def type_heavydb_daytimeinterval(context):
    def typer(arg):
        if isinstance(arg, nb_types.Integer):
            return typesystem.Type.fromobject("DayTimeInterval").tonumba()

    return typer


@extending.lower_builtin(DayTimeInterval, nb_types.Integer)
def heavydb_daytimeinterval_int_ctor(context, builder, sig, args):
    timeval = args[0]
    typ = sig.return_type
    dti = cgutils.create_struct_proxy(typ)(context, builder)
    dti.timeval = timeval
    return dti._getvalue()


@extending.overload(operator.mul)
def ol_day_time_interval_operator_mul(a, t):
    if isinstance(a, DayTimeIntervalNumbaType) and isinstance(t, nb_types.Integer):

        def impl(a, t):
            return DayTimeInterval(a.timeval * t)

        return impl


@extending.overload(operator.eq)
def ol_day_time_interval_operator_eq(a, b):
    if isinstance(a, DayTimeIntervalNumbaType) and isinstance(
        b, DayTimeIntervalNumbaType
    ):

        def impl(a, b):
            return a.timeval == b.timeval

        return impl


@extending.overload(operator.ne)
def ol_day_time_interval_operator_ne(a, b):
    if isinstance(a, DayTimeIntervalNumbaType) and isinstance(
        b, DayTimeIntervalNumbaType
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
            "int64 DateAddHighPrecisionNullable(int64, int64, int64, int32, int64)"
        )
    else:
        raise UnsupportedError('DateAddHighPrecisionNullable is not supported on GPU')

    if isinstance(a, DayTimeIntervalNumbaType) and isinstance(b, TimestampNumbaType):

        def impl(a, b):
            t = DateAddHighPrecisionNullable(
                daMILLISECOND, a.timeval, b.time, 9, i64_null_val
            )
            return Timestamp(t)

        return impl
    elif isinstance(a, TimestampNumbaType) and isinstance(b, DayTimeIntervalNumbaType):

        def impl(a, b):
            t = DateAddHighPrecisionNullable(
                daMILLISECOND, b.timeval, a.time, 9, i64_null_val
            )
            return Timestamp(t)

        return impl


@extending.overload_method(DayTimeIntervalNumbaType, "numStepsBetween")
def ol_day_time_interval_numStepBetween_method(dti, begin, end):
    if isinstance(begin, TimestampNumbaType) and isinstance(end, TimestampNumbaType):

        def impl(dti, begin, end):
            timeval = dti.timeval
            if (timeval > 0 and end.time < begin.time) or (
                timeval < 0 and end.time > begin.time
            ):
                return -1

            diff = nb_types.int64(end.time - begin.time)
            asNanoSecs = timeval * kMicroSecsPerSec
            return diff // asNanoSecs
        return impl
