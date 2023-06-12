
Environment variables
=====================

Debugging
---------

.. envvar:: RBC_DEBUG

    If set to non-zero, prints out all possible debug information
    during function compilation and remote execution.

.. envvar:: RBC_DEBUG_NRT

    If set to non-zero, insert debug statements to our implementation
    of Numba Runtime (NRT).

.. envvar:: RBC_ENABLE_NRT

    If set to 0, globally disable inserting the Numba Runtime (NRT) module.
    Default is 1.
