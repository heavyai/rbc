.. currentmodule:: rbc

=============
API reference
=============

This page provides an auto-generated summary of RBC's API. For more details and
examples, refer to the relevant chapters in the main part of the documentation
and check the Notebooks folder in the main repository.

Top-level functions
===================

.. autosummary::
    :toctree: generated/


Array API
=========

.. autosummary::
    :toctree: generated/


Externals
=========

.. autosummary::
    :toctree: generated/


HeavyDB Backend
===============

The table below contains the data structures available for the HeavyDB backend.
It should be noticed that the following types are not regular Python types but
`Numba types <https://numba.readthedocs.io/en/stable/reference/types.html>`__.

The set of types below are only materialize inside the HeavyDB SQL Engine. Thus,
one cannot create and use them inside the REPL, for instance.


.. autosummary::
    :toctree: generated/

    heavydb.Array
    heavydb.Column
    heavydb.ColumnArray
    heavydb.ColumnListArray
    heavydb.ColumnList
    heavydb.TextEncodingDict
    heavydb.TextEncodingNone
    heavydb.Timestamp
    heavydb.DayTimeInterval
    heavydb.YearMonthTimeInterval
    heavydb.TableFunctionManager
    heavydb.RowFunctionManager
    heavydb.StringDictionaryProxy
