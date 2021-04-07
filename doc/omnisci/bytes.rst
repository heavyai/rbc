.. Omnisci Bytes:

Bytes
=====

.. py:class:: Bytes

    .. rubric:: Methods

    .. py:method:: __init__(self: Bytes, n: int)
    .. py:method:: __getitem__(self: Bytes, idx: int) -> int
    .. py:method:: __setitem__(self: Bytes, idx: int, value: int)

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
