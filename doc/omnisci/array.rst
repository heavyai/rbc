.. Omnisci Array:

Array
=====

.. py:class:: Array<T>
    
    In HeavyDB, an Array with a type `T` is represented as follows:

    .. code-block:: C

        {
            T* data,
            int64_t size,
            int8_t is_null,
        }

    Array is a contiguous block of memory that behaves like a python list.

    .. rubric:: Methods

    .. py:method:: __init__(self, size: int, dtype: numba.types.Type) -> None

        Creates a new Array with the given size and dtype.
        Any `scalar <https://numba.readthedocs.io/en/stable/reference/types.html#numbers>`__
        type is supported as a dtype. 

    .. py:method:: __getitem__(self, idx: int) -> T

    .. py:method:: __setitem__(self, idx: int, value: T) -> None

    .. py:method:: is_null() -> bool

        Checks if the array is null


    .. rubric:: Functions

    .. py:function:: len(arr: Array) -> int

        Returns the size property


.. code-block:: Python

    from numba import types
    from rbc.omnisci_backend import Array

    @omnisci('int64[](int64)')
    def create_and_fill_array(size):
        arr = Array(size, types.int64)
        for i in range(size):
            a[i] = i
        return a