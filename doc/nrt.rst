
Numba Runtime
=============

RBC includes a simplified implementation of the Numba Runtime written in pure
LLVM, which differs from the original C-written implementation. This approach
enables RBC to extend its support to a broader range of Python datatypes,
including lists, sets, and strings, which was not possible with the previous
implementation. Python ``dict`` is not supported as its implementation is
written in C.

List methods supported
----------------------

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


Set methods supported
---------------------

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


String methods supported
------------------------

* [X] 'capitalize'
* [X] 'casefold'
* [X] 'center'
* [X] 'count'
* [ ] 'encode'
* [X] 'endswith'
* [X] 'expandtabs'
* [X] 'find'
* [ ] 'format'
* [ ] 'format_map'
* [X] 'index'
* [X] 'isalnum'
* [X] 'isalpha'
* [X] 'isascii'
* [X] 'isdecimal'
* [X] 'isdigit'
* [X] 'isidentifier'
* [X] 'islower'
* [X] 'isnumeric'
* [X] 'isprintable'
* [X] 'isspace'
* [X] 'istitle'
* [X] 'isupper'
* [X] 'join'
* [X] 'ljust'
* [X] 'lower'
* [X] 'lstrip'
* [ ] 'maketrans'
* [ ] 'partition'
* [X] 'removeprefix'
* [X] 'removesuffix'
* [X] 'replace'
* [X] 'rfind'
* [X] 'rindex'
* [X] 'rjust'
* [ ] 'rpartition'
* [X] 'rsplit'
* [X] 'rstrip'
* [X] 'split'
* [X] 'splitlines'
* [X] 'startswith'
* [X] 'strip'
* [X] 'swapcase'
* [X] 'title'
* [ ] 'translate'
* [X] 'upper'
* [X] 'zfill'

Additionally, one can convert a text encoding none type to a python string using
``TextEncodingNone.to_string`` method.
