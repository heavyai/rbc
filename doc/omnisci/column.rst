.. Omnisci Column:

.. role:: py(code)
   :language: python

Column
======

.. py:class:: Column 
    
    In OmniSciDB, a Column with a type `T` is represented as follows:

    .. code-block:: C

        template typename<T>
        struct Column {
            T* ptr,           // row data
            int64_t sz,       // row count
        }

    Unlike an array, a column cannot be null but its members can.

    .. rubric:: Methods

    .. py:method:: __getitem__(self, idx: int) -> T

        Returns :py:`column[idx]`

    .. py:method:: __setitem__(self, idx: int, value: T) -> None

        Assigns :py:`column[idx] = value`

    .. py:method:: is_null(self, idx: int) -> bool

        Returns if :py:`Column[idx] is None`

    .. py:method:: set_null(self, idx: int) -> None

        Set :py:`Column[idx] = None`

    .. rubric:: Functions

    .. py:function:: len(col: Column) -> int
