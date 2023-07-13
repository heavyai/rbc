(string-dict-proxy)=
# String Dictionary Proxy

The string dictionary proxy provides a convenient way for retrieving encoded
strings within the database. It works by maintaining a lookup table that maps
string values to unique integer identifiers.

## Retrieving the dictionary proxy

### From `RowFunctionManager`

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_text`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.proxy.begin
:end-before: magictoken.udf.proxy.end
:dedent: 8
:linenos:
```

### From `TableFunctionManager`

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_text`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.proxy.begin
:end-before: magictoken.udtf.proxy.end
:dedent: 4
:linenos:
```

Check the [API reference page](API) for a full list of table methods
