# Geometry Types

HeavyDB offers an expanded range of geometry types, encompassing `(Multi)Point`,
`(Multi)LineString`, and `(Multi)Polygon`.

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

<details>
<summary>Example SQL query</summary>

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.geopoint.basic.sql.begin
:end-before: magictoken.udtf.geopoint.basic.sql.end
:dedent: 4
:linenos:
```

</details>
