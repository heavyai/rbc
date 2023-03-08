
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

* [X] NRT_MemInfo_init
* [X] NRT_MemInfo_alloc
* [X] NRT_MemInfo_alloc_safe
* [X] NRT_MemInfo_alloc_dtor
* [X] NRT_MemInfo_alloc_dtor_safe
* [X] NRT_Allocate
* [X] NRT_Allocate_External
* [X] NRT_MemInfo_call_dtor
* [X] NRT_incref - Memory is managed by HeavyDB server. Thus, this function
has an empty body
* [X] NRT_decref - Similarly, this function also has an empty body
* [X] NRT_MemInfo_data_fast
* [X] NRT_MemInfo_new
* [X] NRT_MemInfo_new_varsize_dtor
* [X] NRT_MemInfo_varsize_alloc
* [X] NRT_MemInfo_varsize_realloc
* [X] NRT_MemInfo_varsize_free
* [X] NRT_MemInfo_new_varsize
* [X] NRT_MemInfo_alloc_safe_aligned - Calls the unaligned version for now
* [X] NRT_Reallocate - `realloc` is implemented using `allocate_varlen_buffer`
as the former may free previous allocated memory, which result in a double-free
once the UD[T]F finishes execution
* [X] NRT_dealloc
* [X] NRT_Free - Memory is freed upon UD[T]F return. Thus, this function does
not free any memory
* [X] NRT_MemInfo_destroy


How to debug NRT methods
------------------------

By defining the environment variable `RBC_DEBUG_NRT`, RBC will enable debug
statements to each function call made to NRT.


How to generate `unicodetype_db.ll`
-----------------------------------

Alongside with NRT, unicode strings also required functions which are
implemented using C, in Numba. In RBC, those functions were copied to
`unicodetype_db.h`. To generate the `.ll` file, simply update the `#include`
statement on line 2, and run the clang to generate the bitcode file:

.. code-block:: bash

    $ clang -S -emit-llvm -O2 unicodetype_db.h -o unicodetype_db.ll


List methods tested
-------------------

* [X] list.append
* [X] list.extend
* [X] list.insert
* [X] list.remove
* [X] list.pop
* [X] list.clear
* [X] list.index
* [X] list.count
* [X] list.sort
* [X] list.reverse
* [X] list.copy

Additionally, one can convert an array to a list with ``Array.to_list`` method.


Set methods tested
------------------

* [X] set.add
* [X] set.clear
* [X] set.copy
* [X] set.difference
* [X] set.difference_update
* [X] set.discard
* [X] set.intersection
* [X] set.intersection_update
* [X] set.isdisjoint
* [X] set.issubset
* [X] set.issuperset
* [X] set.pop
* [X] set.remove
* [X] set.symmetric_difference
* [X] set.symmetric_difference_update
* [ ] set.union
* [X] set.update


String methods tested
---------------------

* [X] string.capitalize
* [X] string.casefold
* [X] string.center
* [X] string.count
* [ ] string.encode
* [X] string.endswith
* [X] string.expandtabs
* [X] string.find
* [ ] string.format
* [ ] string.format_map
* [X] string.index
* [X] string.isalnum
* [X] string.isalpha
* [X] string.isascii
* [X] string.isdecimal
* [X] string.isdigit
* [X] string.isidentifier
* [X] string.islower
* [X] string.isnumeric
* [X] string.isprintable
* [X] string.isspace
* [X] string.istitle
* [X] string.isupper
* [X] string.join
* [X] string.ljust
* [X] string.lower
* [X] string.lstrip
* [ ] string.maketrans
* [ ] string.partition
* [X] string.removeprefix
* [X] string.removesuffix
* [X] string.replace
* [X] string.rfind
* [X] string.rindex
* [X] string.rjust
* [ ] string.rpartition
* [X] string.rsplit
* [X] string.rstrip
* [X] string.split
* [X] string.splitlines
* [X] string.startswith
* [X] string.strip
* [X] string.swapcase
* [X] string.title
* [ ] string.translate
* [X] string.upper
* [X] string.zfill

Additionally, one can convert a text encoding none type to a python string using
``TextEncodingNone.to_string`` method.
