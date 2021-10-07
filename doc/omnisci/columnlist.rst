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

    .. py:method:: __getitem__(self, idx: int) -> Column


    .. rubric:: Attributes

    .. py:attribute:: ncols -> int

        Returns :py:`ColumnList.length`

    .. py:attribute:: nrows -> int

        Returns :py:`ColumnList.size`