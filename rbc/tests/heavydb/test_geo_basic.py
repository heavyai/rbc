from rbc import heavydb


def test_column_geo_rewire():
    # ensure column rewiring works
    column_geo = set([t for t in dir(heavydb) if t.startswith('Geo')])
    expected_set = set(['GeoPoint', 'GeoMultiPoint',
                        'GeoLineString', 'GeoMultiLineString',
                        'GeoPolygon', 'GeoMultiPolygon'])
    assert len(column_geo) == len(expected_set)
    assert column_geo == expected_set
