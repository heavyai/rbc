# Column Type

The Column type provide the structure and organization for storing and
retrieving data within HeavyDB.

### Basic usage

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_column_power`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.column.basic.begin
:end-before: magictoken.udtf.column.basic.end
:dedent: 4
:linenos:
```

<details>
<summary>Example SQL query</summary>

```sql
SELECT * FROM TABLE(udtf_power(
    cursor(SELECT * FROM TABLE(generate_series(1, 5)),
    3
));
```

</details>
