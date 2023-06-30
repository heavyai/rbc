
## Raising exceptions

Exceptions in HeavyDB are quite different from the ones used in Python. In RBC
code, you signal to the database an exception happened by calling a specific
method (`error_message`) in the runner manager.

### In a UDTF

```python
@heavydb('int32(TableFunctionManager, Column<int>, OutputColumn<int>)')
def udtf_copy(mgr, inp, out):
    size = len(inp)
    if size > 5:
        # error message must be known at compile-time
        return mgr.error_message('Can only copy up to 5 rows')

    mgr.set_output_row_size(size)
    for i in range(size):
        out[i] = inp[i]
    return size
```

### In a UDF:

It is currently not possible to raise an exception in a UDF. The server must
implement support for it first before RBC can support it.
