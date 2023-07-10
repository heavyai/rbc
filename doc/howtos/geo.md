# Geometry Types

HeavyDB offers an extensive range of geometry types, encompassing
`(Multi)Point`, `(Multi)LineString`, and `(Multi)Polygon`.

## Basics

### `GeoPoint`

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.geopoint.basic.begin
:end-before: magictoken.udtf.geopoint.basic.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.geopoint.basic.sql.begin
:end-before: magictoken.udtf.geopoint.basic.sql.end
:dedent: 4
:linenos:
```

:::


### `GeoMultiPoint`

`MultiPoint` works a bit different than `Point`. They are created by calling
the `.from_coords` method.

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geomultipoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.mp.basic.begin
:end-before: magictoken.udtf.mp.basic.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.mp.basic.sql.begin
:end-before: magictoken.udtf.mp.basic.sql.end
:dedent: 4
:linenos:
```

:::