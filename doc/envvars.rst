
Environment variables
=====================

Debugging
---------

.. envvar:: RBC_DEBUG

    If set to non-zero, prints out all possible debug information
    during function compilation and remote execution.

.. envvar:: RBC_DEBUG_NRT

    If set to non-zero, insert debug statements to our implementation
    of Numba Runtime (NRT)

.. envvar:: DISABLE_LLVM_MISMATCH_ERROR

    If set to non-zero, RBC will not raise an error on Numba/HeavyDB LLVM
    mismatch version. Numba 0.57 uses LLVM 14 and may produce IR that
    older versions of HeavyDB (< 7.0) cannot read.
