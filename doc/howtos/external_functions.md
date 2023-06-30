
## Defining and using external functions

The `external` keyword provide a way of calling C functions defined in other
libraries within python code.

```python
from rbc.external import external
cmath_abs = external('int64 abs(int64)')

@heavydb('int64(int64)')
def apply_abs(x):
    return cmath_abs(x)

heavydb.sql_execute('SELECT apply_abs(-3);')
```

RBC already exposes a small set of C functions from the C stdlib. Check the API
reference page for more details.

### Using `printf`

```python
from rbc.externals.stdio import printf

@heavydb('int64(int64)')
def power_2(x):
    # This message will show in the heavydb logs
    printf("input number: %d\n", x)
    return x * x

heavydb.sql_execute('SELECT power_2(3);')
```