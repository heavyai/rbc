
## Using templates

Templates are generic types used in the decorator `@heavydb`. Templating allows
the target function to accept different data types for its arguments.

Assuming you already have a [connection](heavydb-connect) to the HeavyDB server:

```python
@heavydb('Z(T, Z)', T=['int32', 'float'], Z=['int64', 'double'])
def add(a, b):
    return a + b
```

In the case above, the template arguments `T` and `Z`, are specified within the
decorator, indicating the valid data types that can be used for the `add`
function.
