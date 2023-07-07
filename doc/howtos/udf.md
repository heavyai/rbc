# User Defined Functions (UDFs)

## Basics

UDFs are function that operate at row-level. That is, they receive as input a
single row at a time.

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.begin
:end-before: magictoken.udf.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_column`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.sql.begin
:end-before: magictoken.udf.sql.end
:dedent: 4
:linenos:
```

:::


## Multiple signatures

Defining UDFs with multiple signatures

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.multiple_signatures.begin
:end-before: magictoken.udf.multiple_signatures.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_column`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.multiple_signatures.sql.begin
:end-before: magictoken.udf.multiple_signatures.sql.end
:dedent: 4
:linenos:
```

:::