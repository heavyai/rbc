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
            int32_t dict_id,  // dictionary id
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

    .. py:method:: is_dict_encoded(self) -> bool

        Returns :py:`True` if the Column holds dictionary encoded string data

    .. py:method:: get_dict_id(self) -> int32_t

        Returns the dictionary id if :py:meth:`Column.is_dict_encoded` evaluates to true,
        otherwise returns :py:`-1`

    .. rubric:: Functions

    .. py:function:: len(col: Column) -> int
    