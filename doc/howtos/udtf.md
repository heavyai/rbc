# User Defined Table Functions (UDTFs)

UDTFs are functions that operate on a column-level basis. In simpler terms,
UDTFs take a set of columns as input and return a set of columns as output.

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udtf`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.begin
:end-before: magictoken.udtf.end
:dedent: 4
:linenos:
```


:::{dropdown} Example SQL Query
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udtf`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.sql.begin
:end-before: magictoken.udtf.sql.end
:dedent: 4
:linenos:
```
:::