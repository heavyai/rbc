
# Defining and using external functions

## cmath `abs`

The `external` keyword provide a way of calling C functions defined in other
libraries within python code.

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_external_functions`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.external_functions.abs.begin
:end-before: magictoken.external_functions.abs.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_external_functions`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.external_functions.abs.sql.begin
:end-before: magictoken.external_functions.abs.sql.end
:dedent: 4
:linenos:
```
:::

RBC already exposes a small set of C functions from the C stdlib. Check the API
reference page for more details.

## Using `printf`

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_external_functions`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.external_functions.printf.begin
:end-before: magictoken.external_functions.printf.end
:dedent: 4
:linenos:
```

:::{dropdown} Example SQL Query
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_external_functions`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.external_functions.printf.sql.begin
:end-before: magictoken.external_functions.printf.sql.end
:dedent: 4
:linenos:
```
:::