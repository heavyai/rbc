
## Stricting a function to run in a specific device (CPU/GPU)

Assuming you already have a [connection](heavydb-connect) to the HeavyDB server:

```python
@heavydb('int32(int32, int32)', devices=['CPU', 'GPU'])
def add(a, b):
    return a + b
```

By default, both devices are used if available. Otherwise, only the CPU is used.