"""RBC Timestamp type that corresponds to HeavyDB type timestamp."""
import operator
import numpy as np
from rbc import typesystem
from rbc.heavydb import HeavyDBMetaType
from rbc.external import external
from rbc.targetinfo import TargetInfo
from numba.core import extending, cgutils, datamodel
from numba.core import types as nb_types
from llvmlite import ir
from typing import Union

__all__ = ['HeavyDBTimestampType', 'Timestamp', 'TimestampNumbaType']


kNanoSecsPerSec = int(np.timedelta64(1, 's') / np.timedelta64(1, 'ns'))
kMicroSecsPerSec = int(np.timedelta64(1, 's') / np.timedelta64(1, 'us'))
kMilliSecsPerSec = int(np.timedelta64(1, 's') / np.timedelta64(1, 'ms'))
kSecsPerMin = 60
kMinsPerHour = 60
kHoursPerDay = 24
kSecsPerHour = int(np.timedelta64(1, 'h') / np.timedelta64(1, 's'))
kSecsPerDay = int(np.timedelta64(1, 'D') / np.timedelta64(1, 's'))
kDaysPerWeek = 7
kMonsPerYear = 12


class Timestamp(metaclass=HeavyDBMetaType):
    '''RBC Timestamp type that corresponds to HeavyDB type TIMESTAMP.

    .. code-block:: c
        struct Timestamp {
            int64_t time;
        };

    .. code-block:: python
        from rbc.heavydb import Timestamp
        @heavydb('int32_t(Column<Timestamp>, RowMultiplier, Column<int64_t>')
        def get_years(column_times, m, column_hours):
            for i in range(len(column_times)):
                column_hours[i] = column_times[i].getHours()
            return len(column_times)
    '''

    def __init__(self, time: Union[int, 'nb_types.LiteralString']) -> 'Timestamp':
        pass

    def truncateToMicroseconds(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to microseconds
        """
        pass

    def truncateToMilliseconds(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to milliseconds
        """
        pass

    def truncateToSeconds(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to seconds
        """
        pass

    def truncateToMinutes(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to minutes

        .. note::
            Only available on ``CPU``
        """
        pass

    def truncateToHours(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to hours

        .. note::
            Only available on ``CPU``
        """
        pass

    def truncateToDay(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to day

        .. note::
            Only available on ``CPU``
        """
        pass

    def truncateToMonth(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to month

        .. note::
            Only available on ``CPU``
        """
        pass

    def truncateToYear(self) -> 'Timestamp':
        """
        Return a new  ``Timestamp`` with ``time`` truncated to year

        .. note::
            Only available on ``CPU``
        """
        pass

    def getMicroseconds(self) -> int:
        """
        Get ``time`` as microseconds

        .. note::
            Only available on ``CPU``
        """
        pass

    def getMilliseconds(self) -> int:
        """
        Get ``time`` as milliseconds

        .. note::
            Only available on ``CPU``
        """
        pass

    def getSeconds(self) -> int:
        """
        Get ``time`` as seconds

        .. note::
            Only available on ``CPU``
        """
        pass

    def getMinutes(self) -> int:
        """
        Get ``time`` as minutes

        .. note::
            Only available on ``CPU``
        """
        pass

    def getHours(self) -> int:
        """
        Get ``time`` as hours

        .. note::
            Only available on ``CPU``
        """
        pass

    def getDay(self) -> int:
        """
        Get ``time`` as day

        .. note::
            Only available on ``CPU``
        """
        pass

    def getMonth(self) -> int:
        """
        Get ``time`` as month

        .. note::
            Only available on ``CPU``
        """
        pass

    def getYear(self) -> int:
        """
        Get ``time`` as year

        .. note::
            Only available on ``CPU``
        """
        pass

    def __eq__(self, other: 'Timestamp') -> bool:
        """
        """
        pass

    def __ne__(self, other: 'Timestamp') -> bool:
        """
        """
        pass

    def __lt__(self, other: 'Timestamp') -> bool:
        """
        """
        pass

    def __le__(self, other: 'Timestamp') -> bool:
        """
        """
        pass

    def __gt__(self, other: 'Timestamp') -> bool:
        """
        """
        pass

    def __ge__(self, other: 'Timestamp') -> bool:
        """
        """
        pass

    def __add__(self, other: 'Timestamp') -> 'Timestamp':
        """
        """
        pass

    def __sub__(self, other: 'Timestamp') -> 'Timestamp':
        """
        """
        pass

    def __mul__(self, other: 'Timestamp') -> 'Timestamp':
        """
        """
        pass

    def __floordiv__(self, other: 'Timestamp') -> 'Timestamp':
        """
        """
        pass

    def __truediv__(self, other: 'Timestamp') -> 'Timestamp':
        """
        """
        pass


class TimestampNumbaType(nb_types.Type):
    bitwidth = 64

    def __init__(self):
        super().__init__(name='Timestamp')


@extending.register_model(TimestampNumbaType)
class IntervalModel(datamodel.models.StructModel):
    def __init__(self, dmm, fe_type):
        members = [
            ('time', nb_types.int64),
        ]
        datamodel.models.StructModel.__init__(self, dmm, fe_type, members)


extending.make_attribute_wrapper(TimestampNumbaType, 'time', 'time')


class HeavyDBTimestampType(typesystem.Type):
    """Typesystem type class for HeavyDB timestamp structures.
    """
    @property
    def __typesystem_type__(self):
        return typesystem.Type.fromstring('{int64 time}').params(
            name='Timestamp', NumbaType=TimestampNumbaType)

    def tonumba(self, bool_is_int8=None):
        return TimestampNumbaType()

    def tostring(self, use_typename=False, use_annotation=True, use_name=True,
                 use_annotation_name=False, _skip_annotation=False):
        return 'Timestamp'


@extending.type_callable(Timestamp)
def type_heavydb_timestamp(context):
    def typer(arg):
        if isinstance(arg, (nb_types.Integer, nb_types.StringLiteral)):
            return typesystem.Type.fromobject('Timestamp').tonumba()

    return typer


@extending.lower_builtin(Timestamp, nb_types.Integer)
def heavydb_timestamp_int_ctor(context, builder, sig, args):
    time = args[0]
    typ = sig.return_type
    timestamp = cgutils.create_struct_proxy(typ)(context, builder)
    timestamp.time = time
    return timestamp._getvalue()


@extending.lower_builtin(Timestamp, nb_types.StringLiteral)
def heavydb_timestamp_literal_string_ctor(context, builder, sig, args):
    i64 = ir.IntType(64)
    time = i64(np.datetime64(sig.args[0].literal_value).astype('long'))
    typ = sig.return_type
    timestamp = cgutils.create_struct_proxy(typ)(context, builder)
    timestamp.time = time
    return timestamp._getvalue()


@extending.overload_method(TimestampNumbaType, "getYear")
def heavydb_timestamp_getYear(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_year = external('int64 extract_year(int64)|cpu')

        def impl(timestamp):
            return extract_year(timestamp.time // kNanoSecsPerSec)
        return impl


@extending.overload_method(TimestampNumbaType, "getMonth")
def heavydb_timestamp_getMonth(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_month = external('int64 extract_month(int64)|cpu')

        def impl(timestamp):
            return extract_month(timestamp.time // kNanoSecsPerSec)
        return impl


@extending.overload_method(TimestampNumbaType, "getDay")
def heavydb_timestamp_getDay(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_day = external('int64 extract_day(int64)|cpu')

        def impl(timestamp):
            return extract_day(timestamp.time // kNanoSecsPerSec)
        return impl


@extending.overload_method(TimestampNumbaType, "getHours")
def heavydb_timestamp_getHours(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_hour = external('int64 extract_hour(int64)|cpu')

        def impl(timestamp):
            return extract_hour(timestamp.time // kNanoSecsPerSec)
        return impl


@extending.overload_method(TimestampNumbaType, "getMinutes")
def heavydb_timestamp_getMinutes(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_minute = external('int64 extract_minute(int64)|cpu')

        def impl(timestamp):
            return extract_minute(timestamp.time // kNanoSecsPerSec)
        return impl


@extending.overload_method(TimestampNumbaType, "getSeconds")
def heavydb_timestamp_getSeconds(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_second = external('int64 extract_second(int64)|cpu')

        def impl(timestamp):
            return extract_second(timestamp.time // kNanoSecsPerSec)
        return impl


@extending.overload_method(TimestampNumbaType, "getMilliseconds")
def heavydb_timestamp_getMilliseconds(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_millisecond = external('int64 extract_millisecond(int64)|cpu')

        def impl(timestamp):
            return extract_millisecond(
                timestamp.time // (kNanoSecsPerSec // kMilliSecsPerSec))
        return impl


@extending.overload_method(TimestampNumbaType, "getMicroseconds")
def heavydb_timestamp_getMicroseconds(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_microsecond = external('int64 extract_microsecond(int64)|cpu')

        def impl(timestamp):
            return extract_microsecond(
                timestamp.time // (kNanoSecsPerSec // kMicroSecsPerSec))
        return impl


@extending.overload_method(TimestampNumbaType, "getNanoseconds")
def heavydb_timestamp_getNanoseconds(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        extract_nanosecond = external('int64 extract_nanosecond(int64)|cpu')

        def impl(timestamp):
            return extract_nanosecond(timestamp.time)
        return impl


@extending.overload_method(TimestampNumbaType, "truncateToYear")
def heavydb_timestamp_truncateToYear(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        datetrunc_year = external('int64 datetrunc_year(int64)|cpu')

        def impl(timestamp):
            timeval = datetrunc_year(timestamp.time // kNanoSecsPerSec) * kNanoSecsPerSec
            return Timestamp(timeval)
        return impl


@extending.overload_method(TimestampNumbaType, "truncateToMonth")
def heavydb_timestamp_truncateToMonth(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        datetrunc_month = external('int64 datetrunc_month(int64)|cpu')

        def impl(timestamp):
            timeval = datetrunc_month(timestamp.time // kNanoSecsPerSec) * kNanoSecsPerSec
            return Timestamp(timeval)
        return impl


@extending.overload_method(TimestampNumbaType, "truncateToDay")
def heavydb_timestamp_truncateToDay(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        datetrunc_day = external('int64 datetrunc_day(int64)|cpu')

        def impl(timestamp):
            timeval = datetrunc_day(timestamp.time // kNanoSecsPerSec) * kNanoSecsPerSec
            return Timestamp(timeval)
        return impl


@extending.overload_method(TimestampNumbaType, "truncateToHours")
def heavydb_timestamp_truncateToHour(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        datetrunc_hour = external('int64 datetrunc_hour(int64)|cpu')

        def impl(timestamp):
            timeval = datetrunc_hour(timestamp.time // kNanoSecsPerSec) * kNanoSecsPerSec
            return Timestamp(timeval)
        return impl


@extending.overload_method(TimestampNumbaType, "truncateToMinutes")
def heavydb_timestamp_truncateToMinutes(timestamp):
    target_info = TargetInfo()
    if target_info.is_cpu:
        datetrunc_minute = external('int64 datetrunc_minute(int64)|cpu')

        def impl(timestamp):
            timeval = datetrunc_minute(timestamp.time // kNanoSecsPerSec) * kNanoSecsPerSec
            return Timestamp(timeval)
        return impl


@extending.overload_method(TimestampNumbaType, "truncateToSeconds")
def heavydb_timestamp_truncateToSeconds(timestamp):

    def impl(timestamp):
        timeval = (timestamp.time // kNanoSecsPerSec) * kNanoSecsPerSec
        return Timestamp(timeval)
    return impl


@extending.overload_method(TimestampNumbaType, "truncateToMilliseconds")
def heavydb_timestamp_truncateToMilliseconds(timestamp):

    def impl(timestamp):
        timeval = (timestamp.time // kMicroSecsPerSec) * kMicroSecsPerSec
        return Timestamp(timeval)
    return impl


@extending.overload_method(TimestampNumbaType, "truncateToMicroseconds")
def heavydb_timestamp_truncateToMicroseconds(timestamp):

    def impl(timestamp):
        timeval = (timestamp.time // kMilliSecsPerSec) * kMilliSecsPerSec
        return Timestamp(timeval)
    return impl


def overload_binary_cmp_op(op, retty):

    def heavydb_operator_impl(a, b):
        if isinstance(a, TimestampNumbaType) and isinstance(b, TimestampNumbaType):
            def impl(a, b):
                return retty(nb_types.int64(op(a.time, b.time)))
            return impl

    decorate = extending.overload(op)

    def wrapper(overload_func):
        return decorate(heavydb_operator_impl)

    return wrapper


@overload_binary_cmp_op(operator.eq, bool)
@overload_binary_cmp_op(operator.ne, bool)
@overload_binary_cmp_op(operator.lt, bool)
@overload_binary_cmp_op(operator.le, bool)
@overload_binary_cmp_op(operator.gt, bool)
@overload_binary_cmp_op(operator.ge, bool)
@overload_binary_cmp_op(operator.truediv, Timestamp)
@overload_binary_cmp_op(operator.floordiv, Timestamp)
@overload_binary_cmp_op(operator.add, Timestamp)
@overload_binary_cmp_op(operator.sub, Timestamp)
@overload_binary_cmp_op(operator.mul, Timestamp)
def heavydb_timestamp(a, b):
    pass
