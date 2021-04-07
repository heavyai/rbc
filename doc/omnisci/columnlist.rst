.. Omnisci ColumnList:

ColumnList
==========

.. py:class:: ColumnList

    .. code-block:: C

        template typename<T>
        struct ColumnList {
            T** ptrs;   // ptrs to columns data
            int64_t length;  // the length of columns list
            int64_t size;    // the size of columns
        }
 
    
    .. rubric:: Methods

    .. py:method:: ncols(self: ColumnList) -> int

        Returns :py:`ColumnList.length`

    .. py:method:: nrows(self: ColumnList) -> int

        Returns :py:`ColumnList.size`
    
    .. py:method:: __getitem__(self: ColumnList, idx: int) -> Column
