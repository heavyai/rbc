# Table/Row Function Manager

The function manager in HeavyDB provides a convenient mechanism of handling the
state of user-defined functions. It can perform various tasks such as
allocating memory for output buffers, retrieve dictionary encoded strings in
the [dictionary proxy](string-dict-proxy), and raise exceptions.

## Table Function Manager

### Basic usage

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_mgr`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.mgr.basic.begin
:end-before: magictoken.udtf.mgr.basic.end
:dedent: 4
:linenos:
```

### Retrieving the dictionary string proxy

When the Column has type `TextEncodingDict`, users can access the dictionary
string proxy by calling the `string_dict_proxy` attribute:

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_udtf_string_proxy`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udtf.proxy.begin
:end-before: magictoken.udtf.proxy.end
:dedent: 4
:linenos:
```

For additional information and references, please refer to the [API](API) page
and the dedicated [how-to](string-dict-proxy) page on the string dictionary
proxy in HeavyDB.

## Row Function Manager

```{literalinclude} ../../rbc/tests/heavydb/test_howtos.py
:language: python
:caption: from ``test_mgr`` of ``rbc/tests/heavydb/test_howtos.py``
:start-after: magictoken.udf.mgr.basic.begin
:end-before: magictoken.udf.mgr.basic.end
:dedent: 8
:linenos:
```