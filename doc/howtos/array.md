# Array type

Arrays provide a convenient way to represent a sequence of values of the same
type.

## Basics

### Building an array

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_array`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.array.new.begin
:end-before: magictoken.udf.array.new.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.array.new.sql.begin
:end-before: magictoken.udf.array.new.sql.end
:dedent: 4
:linenos:
```

:::


### Computing the length of an array

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_array`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.array.length.begin
:end-before: magictoken.udf.array.length.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.array.length.sql.begin
:end-before: magictoken.udf.array.length.sql.end
:dedent: 4
:linenos:
```

:::

### Using the [array_api](https://data-apis.org/array-api/2022.12/)

RBC partially implements the array api standard. For a list of supported
functions, check the [API](API) page.

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_array`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.array.array_api.begin
:end-before: magictoken.udf.array.array_api.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_geopoint`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.array.array_api.sql.begin
:end-before: magictoken.udf.array.array_api.sql.end
:dedent: 4
:linenos:
```

:::