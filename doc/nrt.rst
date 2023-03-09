
Numba Runtime
=============

RBC includes a simplified implementation of the Numba Runtime written in pure
LLVM, which differs from the original C-written implementation. This approach
enables RBC to extend its support to a broader range of Python datatypes,
including lists, sets, and strings, which was not possible with the previous
implementation. Python ``dict`` is not supported as its implementation is
written in C.


List of NRT functions implemented
---------------------------------

The RBC project includes some functions from the NRT module, with changes made
as necessary to fit our specific use case. Here is a list of the functions we
have implemented, along with any relevant comments.

* ✔️ ``NRT_MemInfo_init``
* ✔️ ``NRT_MemInfo_alloc``
* ✔️ ``NRT_MemInfo_alloc_safe``
* ✔️ ``NRT_MemInfo_alloc_dtor``
* ✔️ ``NRT_MemInfo_alloc_dtor_safe``
* ✔️ ``NRT_Allocate``
* ✔️ ``NRT_Allocate_External``
* ✔️ ``NRT_MemInfo_call_dtor``
* ✔️ ``NRT_incref`` - Memory is managed by HeavyDB server. Thus, this function has an empty body
* ✔️ ``NRT_decref`` - Similarly, this function also has an empty body
* ✔️ ``NRT_MemInfo_data_fast``
* ✔️ ``NRT_MemInfo_new``
* ✔️ ``NRT_MemInfo_new_varsize_dtor``
* ✔️ ``NRT_MemInfo_varsize_alloc``
* ✔️ ``NRT_MemInfo_varsize_realloc``
* ✔️ ``NRT_MemInfo_varsize_free``
* ✔️ ``NRT_MemInfo_new_varsize``
* ✔️ ``NRT_MemInfo_alloc_safe_aligned`` - Calls the unaligned version for now
* ✔️ ``NRT_Reallocate`` - ``realloc`` is implemented using ``allocate_varlen_buffer`` as the former may free previous allocated memory, which result in a double-free once the UD[T]F finishes execution
* ✔️ ``NRT_dealloc``
* ✔️ ``NRT_Free`` - Memory is freed upon UD[T]F return. Thus, this function does not free any memory
* ✔️ ``NRT_MemInfo_destroy``


How to debug NRT methods
------------------------

By defining the environment variable ``RBC_DEBUG_NRT``, RBC will enable debug
statements to each function call made to NRT.


How to generate ``unicodetype_db.ll``
-------------------------------------

Alongside with NRT, unicode strings also required functions which are
implemented using C, in Numba. In RBC, those functions were copied to
``unicodetype_db.h``. To generate the ``.ll`` file, simply update the
``#include`` statement on line 2, and run the clang to generate the bitcode
file:

.. code-block:: bash

    $ clang -S -emit-llvm -O2 unicodetype_db.h -o unicodetype_db.ll


Supported Python Containers
===========================

List
----

* ✔️ ``list.append``
* ✔️ ``list.extend``
* ✔️ ``list.insert``
* ✔️ ``list.remove``
* ✔️ ``list.pop``
* ✔️ ``list.clear``
* ✔️ ``list.index``
* ✔️ ``list.count``
* ✔️ ``list.sort``
* ✔️ ``list.reverse``
* ✔️ ``list.copy``

Additionally, one can convert an array to a list with ``Array.to_list`` method.


Set
---

* ✔️ ``set.add``
* ✔️ ``set.clear``
* ✔️ ``set.copy``
* ✔️ ``set.difference``
* ✔️ ``set.difference_update``
* ✔️ ``set.discard``
* ✔️ ``set.intersection``
* ✔️ ``set.intersection_update``
* ✔️ ``set.isdisjoint``
* ✔️ ``set.issubset``
* ✔️ ``set.issuperset``
* ✔️ ``set.pop``
* ✔️ ``set.remove``
* ✔️ ``set.symmetric_difference``
* ✔️ ``set.symmetric_difference_update``
* ❌ ``set.union``
* ✔️ ``set.update``


String
------

* ✔️ ``string.capitalize``
* ✔️ ``string.casefold``
* ✔️ ``string.center``
* ✔️ ``string.count``
* ❌ ``string.encode``
* ✔️ ``string.endswith``
* ✔️ ``string.expandtabs``
* ✔️ ``string.find``
* ❌ ``string.format``
* ❌ ``string.format_map``
* ✔️ ``string.index``
* ✔️ ``string.isalnum``
* ✔️ ``string.isalpha``
* ✔️ ``string.isascii``
* ✔️ ``string.isdecimal``
* ✔️ ``string.isdigit``
* ✔️ ``string.isidentifier``
* ✔️ ``string.islower``
* ✔️ ``string.isnumeric``
* ✔️ ``string.isprintable``
* ✔️ ``string.isspace``
* ✔️ ``string.istitle``
* ✔️ ``string.isupper``
* ✔️ ``string.join``
* ✔️ ``string.ljust``
* ✔️ ``string.lower``
* ✔️ ``string.lstrip``
* ❌ ``string.maketrans``
* ❌ ``string.partition``
* ✔️ ``string.removeprefix``
* ✔️ ``string.removesuffix``
* ✔️ ``string.replace``
* ✔️ ``string.rfind``
* ✔️ ``string.rindex``
* ✔️ ``string.rjust``
* ❌ ``string.rpartition``
* ✔️ ``string.rsplit``
* ✔️ ``string.rstrip``
* ✔️ ``string.split``
* ✔️ ``string.splitlines``
* ✔️ ``string.startswith``
* ✔️ ``string.strip``
* ✔️ ``string.swapcase``
* ✔️ ``string.title``
* ❌ ``string.translate``
* ✔️ ``string.upper``
* ✔️ ``string.zfill``

Additionally, one can convert a text encoding none type to a python string using
``TextEncodingNone.to_string`` method.


Examples
--------

Tests are a good reference for using the methods defined above:

* `List <https://github.com/xnd-project/rbc/blob/main/rbc/tests/heavydb/test_nrt_list.py>`_
* `Set <https://github.com/xnd-project/rbc/blob/main/rbc/tests/heavydb/test_nrt_set.py>`_
* `String <https://github.com/xnd-project/rbc/blob/main/rbc/tests/heavydb/test_nrt_string.py>`_
