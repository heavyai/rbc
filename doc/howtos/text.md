# Text types

HeavyDB supports two text encoding options: `TEXT ENCODING NONE` and
`TEXT ENCODING DICT`.

`TEXT ENCODING NONE` stores textual data without compression, while
`TEXT ENCODING DICT` uses dictionary-based encoding to reduce storage
requirements by replacing common words or phrases with shorter codes.
The choice depends on data characteristics and the trade-off between storage
space and encoding/decoding overhead.

## Defining an UDF with Text types:

### Encoding dict
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_text`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.text.dict.begin
:end-before: magictoken.udf.text.dict.end
:dedent: 8
:linenos:
```


### Encoding none
```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udf_text`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.text.none.begin
:end-before: magictoken.udf.text.none.end
:dedent: 8
:linenos:
```
