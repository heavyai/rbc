.. _API:
.. currentmodule:: rbc

=============
API reference
=============

This page provides an auto-generated summary of RBC's API. For more details and
examples, refer to the relevant chapters in the main part of the documentation
and check the Notebooks folder in the main repository.

Top-level functions
-------------------

.. autosummary::
    :toctree: generated/

    ctools
    external
    heavydb
    remotejit
    targetinfo


HeavyDB Backend
---------------

The table below contains the data structures available for the HeavyDB backend.
It should be noticed that the following types are not regular Python types but
`Numba types <https://numba.readthedocs.io/en/stable/reference/types.html>`__.

The set of types below are only materialize inside the HeavyDB SQL Engine. Thus,
one cannot create and use them inside the REPL, for instance.

Array Types
^^^^^^^^^^^

.. autosummary::
    :toctree: generated/

    heavydb.Array


Column Types
^^^^^^^^^^^^

.. autosummary::
    :toctree: generated/

    heavydb.Column
    heavydb.ColumnArray
    heavydb.ColumnListArray
    heavydb.ColumnList
    heavydb.ColumnGeoPoint
    heavydb.ColumnGeoMultiPoint
    heavydb.ColumnGeoLineString
    heavydb.ColumnGeoMultiLineString
    heavydb.ColumnGeoPolygon
    heavydb.ColumnGeoMultiPolygon
    heavydb.ColumnTextEncodingNone


Geo Types
^^^^^^^^^

.. autosummary::
    :toctree: generated/

    heavydb.GeoLineString
    heavydb.GeoMultiPoint
    heavydb.GeoMultiLineString
    heavydb.GeoMultiPolygon
    heavydb.GeoPoint
    heavydb.GeoPolygon


Text-related Types
^^^^^^^^^^^^^^^^^^

.. autosummary::
    :toctree: generated/

    heavydb.TextEncodingDict
    heavydb.StringDictionaryProxy
    heavydb.TextEncodingNone


Row/Table Function Manager
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
    :toctree: generated/

    heavydb.RowFunctionManager
    heavydb.TableFunctionManager


Time-related Types
^^^^^^^^^^^^^^^^^^

.. autosummary::
    :toctree: generated/

    heavydb.DayTimeInterval
    heavydb.Timestamp
    heavydb.YearMonthTimeInterval


Array API
---------

.. autosummary::
    :toctree: generated/

    stdlib.constants
    stdlib.creation_functions
    stdlib.datatypes
    stdlib.data_type_functions
    stdlib.elementwise_functions
    stdlib.linear_algebra_functions
    stdlib.manipulation_functions
    stdlib.searching_functions
    stdlib.set_functions
    stdlib.sorting_functions
    stdlib.statistical_functions
    stdlib.utility_functions


