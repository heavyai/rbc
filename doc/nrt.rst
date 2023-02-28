
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
