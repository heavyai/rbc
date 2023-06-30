
# Stricting a function to run in a specific device (CPU/GPU)

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