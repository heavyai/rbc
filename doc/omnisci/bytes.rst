.. Omnisci Bytes:

Bytes
=====

.. py:class:: Bytes

    In OmniSciDB, Bytes is represented as follows:

    .. code-block:: C

        {
            char* ptr,
            int64_t size,  // when non-negative, Bytes has fixed width.
            int8_t is_null,
        }


    .. rubric:: Methods

    .. py:method:: __init__(self, n: int)
    .. py:method:: __getitem__(self, idx: int) -> int
    .. py:method:: __setitem__(self, idx: int, value: int)

    .. rubric:: Functions

    .. py:function:: len(bytes: Bytes) -> int


Example
=======

.. code-block:: python

    from rbc.omnisci_backend import Bytes

    @omnisci('Bytes(int32, int32)')
    def make_abc(first, n):
        r = Bytes(n)
        for i in range(n):
            r[i] = first + i
        return r
