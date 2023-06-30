
# Using templates

Templates are generic types used in the decorator `@heavydb`. Templating allows
the target function to accept different data types for its arguments.

Assuming you already have a [connection](heavydb-connect) to the HeavyDB server:

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_templates`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.templates.begin
:end-before: magictoken.templates.end
:dedent: 4
:linenos:
```

In the case above, the template arguments `T` and `Z`, are specified within the
decorator, indicating the valid data types that can be used for the `add`
function.
