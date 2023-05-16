import pytest

from rbc.typesystem import Type
from rbc.tests import heavydb_fixture


@pytest.fixture(scope="module")
def heavydb():
    for o in heavydb_fixture(globals(), suffices=[""]):
        yield o


all_types = (
    # Scalars
    "bool8",
    "int8",
    "int16",
    "int32",
    "int64",
    "float32",
    "float64",
    "int8*",
    "int16*",
    "int32*",
    "int64*",
    "float32*",
    "float64*",
    "bool",
    "bool*",
    "Cursor",
    "void",
    "GeoPoint",
    "GeoLineString",
    "GeoPolygon",
    "GeoMultiPolygon",
    "GeoMultiLineString",
    "GeoMultiPoint",
    "TextEncodingNone",
    "TextEncodingDict",
    "Timestamp",
    "DayTimeInterval",
    "YearMonthTimeInterval",
    # Array
    "Array<bool>",
    "Array<int8_t>",
    "Array<int16_t>",
    "Array<int32_t>",
    "Array<int64_t>",
    "Array<float>",
    "Array<double>",
    "Array<TextEncodingDict>",
    "Array<TextEncodingNone>",
    "Array<GeoPoint>",
    "Array<GeoMultiPoint>",
    "Array<GeoLineString>",
    "Array<GeoMultiLineString>",
    "Array<GeoPolygon>",
    "Array<GeoMultiPolygon>",
    # Column
    "Column<bool>",
    "Column<int8_t>",
    "Column<int16_t>",
    "Column<int32_t>",
    "Column<int64_t>",
    "Column<float>",
    "Column<double>",
    "Column<TextEncodingDict>",
    "Column<TextEncodingNone>",
    "Column<Timestamp>",
    "Column<GeoPoint>",
    "Column<GeoMultiPoint>",
    "Column<GeoLineString>",
    "Column<GeoMultiLineString>",
    "Column<GeoPolygon>",
    "Column<GeoMultiPolygon>",
    # ColumnList
    "ColumnList<bool>",
    "ColumnList<int8_t>",
    "ColumnList<int16_t>",
    "ColumnList<int32_t>",
    "ColumnList<int64_t>",
    "ColumnList<float>",
    "ColumnList<double>",
    "ColumnList<TextEncodingDict>",
    "ColumnList<TextEncodingNone>",
    "ColumnList<GeoPoint>",
    "ColumnList<GeoMultiPoint>",
    "ColumnList<GeoLineString>",
    "ColumnList<GeoMultiLineString>",
    "ColumnList<GeoPolygon>",
    "ColumnList<GeoMultiPolygon>",
    # ColumnArray
    "Column<Array<bool>>",
    "Column<Array<int8_t>>",
    "Column<Array<int16_t>>",
    "Column<Array<int32_t>>",
    "Column<Array<int64_t>>",
    "Column<Array<float>>",
    "Column<Array<double>>",
    "Column<Array<TextEncodingDict>>",
    "Column<Array<TextEncodingNone>>",
    "Column<Array<GeoPoint>>",
    "Column<Array<GeoMultiPoint>>",
    "Column<Array<GeoLineString>>",
    "Column<Array<GeoMultiLineString>>",
    "Column<Array<GeoPolygon>>",
    "Column<Array<GeoMultiPolygon>>",
    # ColumnListArray
    "ColumnList<Array<bool>>",
    "ColumnList<Array<int8_t>>",
    "ColumnList<Array<int16_t>>",
    "ColumnList<Array<int32_t>>",
    "ColumnList<Array<int64_t>>",
    "ColumnList<Array<float>>",
    "ColumnList<Array<double>>",
    "ColumnList<Array<TextEncodingDict>>",
    "ColumnList<Array<TextEncodingNone>>",
    "ColumnList<Array<GeoPoint>>",
    "ColumnList<Array<GeoMultiPoint>>",
    "ColumnList<Array<GeoLineString>>",
    "ColumnList<Array<GeoMultiLineString>>",
    "ColumnList<Array<GeoPolygon>>",
    "ColumnList<Array<GeoMultiPolygon>>",
)


def test_len(heavydb):
    msg = 'variable "all_types" requires update'
    assert len(heavydb._get_ext_arguments_map()) == len(all_types), msg


@pytest.mark.parametrize('device', ('cpu', 'gpu'))
@pytest.mark.parametrize("typ", all_types)
def test_type_to_extarg(heavydb, device, typ):
    skiplist = ('Array<Geo', 'Column<Array<Geo', 'ColumnList<Array<Geo',
                'Cursor', 'bool*')

    for skip in skiplist:
        if typ.startswith(skip):
            msg = f'heavydb.type_to_extarg({typ}) not supported.'
            pytest.skip(msg)

    with Type.alias(**heavydb.typesystem_aliases):
        for _device, target_info in heavydb.targets.items():
            if device == _device:
                with target_info:
                    t = Type.fromstring(typ)
                    assert heavydb.type_to_extarg(t) is not None
