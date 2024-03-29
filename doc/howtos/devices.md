# Restricting a function to run only in a specific device (CPU/GPU)

Assuming you already have a [connection](heavydb-connect) to the HeavyDB server:

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_devices`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.devices.begin
:end-before: magictoken.devices.end
:dedent: 4
:linenos:
```

By default, both devices are used if available. Otherwise, only the CPU is used.

:::{dropdown} Example SQL Query
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_devices`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.devices.sql.begin
:end-before: magictoken.devices.sql.end
:dedent: 4
:linenos:
```
:::