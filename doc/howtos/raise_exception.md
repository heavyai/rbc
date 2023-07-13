
# Raising exceptions

Exceptions in HeavyDB are quite different from the ones used in Python. In RBC
code, you signal to the database an exception happened by calling a specific
method (`error_message`) in the runner manager.

## In a UDF:

It is currently not possible to raise an exception in a UDF. The server must
implement support for it first before RBC can support it.

## In a UDTF

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_raise_exception`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.raise_exception.begin
:end-before: magictoken.raise_exception.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_raise_exception`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.raise_exception.sql.begin
:end-before: magictoken.raise_exception.sql.end
:dedent: 4
:linenos:
```
:::