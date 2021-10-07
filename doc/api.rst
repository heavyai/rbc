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

    ctools
    external
    errors
    libfuncs
    omniscidb
    remotejit
    structure_type
    targetinfo
    typesystem
    utils


Externals
=========

.. autosummary::
    :toctree: generated/

    externals.cmath
    externals.libdevice
    externals.macros
    externals.omniscidb
    externals.stdio


OmniSciDB Backend
=================

The table below contains the data structures available for the OmniSciDB backend.
It should be noticed that the following types are not regular Python types but
`Numba types <https://numba.readthedocs.io/en/stable/reference/types.html>`__.

The set of types below are only materialize inside the OmniSciDB SQL Engine. Thus,
one cannot create and use them inside the REPL, for instance.


.. raw:: html

    <table class="longtable docutils align-default">
        <colgroup>
        <col style="width: 10%">
        <col style="width: 90%">
        </colgroup>
        <tbody>
            <tr class="row-odd">
                <td>
                    <p><a class="reference internal" href="omnisci/array.html" title="rbc.omnisci_backend.Array">
                            <code class="xref py py-obj docutils literal notranslate">
                                <span class="pre">rbc.omnisci_backend.Array</span>
                            </code></a></p>
                </td>
                <td><p></p></td>
            </tr>
            <tr class="row-even">
                <td>
                    <p><a class="reference internal" href="omnisci/bytes.html" title="rbc.omnisci_backend.Bytes">
                            <code class="xref py py-obj docutils literal notranslate">
                                <span class="pre">rbc.omnisci_backend.Bytes</span>
                            </code></a></p>
                </td>
                <td><p></p></td>
            </tr>
            <tr class="row-odd">
                <td>
                    <p><a class="reference internal" href="omnisci/column.html" title="rbc.omnisci_backend.Column">
                            <code class="xref py py-obj docutils literal notranslate">
                                <span class="pre">rbc.omnisci_backend.Column</span>
                            </code></a></p>
                </td>
                <td><p></p></td>
            </tr>
            <tr class="row-even">
                <td>
                    <p><a class="reference internal" href="omnisci/outputcolumn.html" title="rbc.omnisci_backend.OutputColumn">
                            <code class="xref py py-obj docutils literal notranslate">
                                <span class="pre">rbc.omnisci_backend.OutputColumn</span>
                            </code></a></p>
                </td>
                <td><p></p></td>
            </tr>
            <tr class="row-odd">
                <td>
                    <p><a class="reference internal" href="omnisci/columnlist.html" title="rbc.omnisci_backend.ColumnList">
                            <code class="xref py py-obj docutils literal notranslate">
                                <span class="pre">rbc.omnisci_backend.ColumnList</span>
                            </code></a></p>
                </td>
                <td><p></p></td>
            </tr>
        </tbody>
    </table>
