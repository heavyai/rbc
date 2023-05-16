import pytest

from contextlib import nullcontext as does_not_raise

from rbc.tests import heavydb_fixture
from rbc.typesystem import Type


@pytest.fixture(scope="module")
def heavydb():
    for o in heavydb_fixture(globals(), suffices=[""]):
        yield o


primitives = (
    "void",
    "bool",
    "bool8",
    "int8",
    "int16",
    "int32",
    "int64",
    "float32",
    "float64",
    "short",
    "int",
    "long",
    "long long",
    "float",
    "double",
    # fixed width integer types
    "int8_t",
    "int16_t",
    "int32_t",
    "int64_t",
)

pointers = tuple(map(lambda T: f"{T}*", primitives))

geo = (
    "GeoPoint",
    "GeoMultiPoint",
    "GeoLineString",
    "GeoMultiLineString",
    "GeoPolygon",
    "GeoMultiPolygon",
)
text = ("TextEncodingNone", "TextEncodingDict")
time = ("Timestamp", "DayTimeInterval", "YearMonthTimeInterval")
scalars = primitives + geo + text + time

cursor = tuple(map(lambda T: f"Cursor<{T}>", scalars))
array = tuple(map(lambda T: f"Array<{T}>", scalars))
column = tuple(map(lambda T: f"Column<{T}>", scalars))
column_list = tuple(map(lambda T: f"ColumnList<{T}>", scalars))
column_array = tuple(map(lambda T: f"Column<{T}>", array))
column_list_array = tuple(map(lambda T: f"ColumnList<{T}>", array))

all_types = scalars + pointers + column + column_list + column_array + column_list_array


@pytest.mark.parametrize("device", ("cpu", "gpu"))
@pytest.mark.parametrize("typ", all_types)
def test_type_to_extarg(heavydb, device, typ):
    skiplist = (
        "Cursor",
        "bool*",
        "void*",
        "<void>",
        "<DayTimeInterval>",
        "<YearMonthTimeInterval>",
        "ColumnList<Timestamp>",
        "Column<Array<Timestamp>>",
        "ColumnList<Array<Timestamp>>",
        "<Geo",
    )

    for skip in skiplist:
        if skip in typ:
            msg = f"heavydb.type_to_extarg({typ}) not supported."
            pytest.skip(msg)

    if device not in heavydb.targets:
        pytest.skip(f'device "{device}" not available')
    target_info = heavydb.targets[device]
    with target_info:
        with Type.alias(**heavydb.typesystem_aliases):
            t = Type.fromstring(typ)
            with does_not_raise():
                heavydb.type_to_extarg(t)
