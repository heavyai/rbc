
Developer Manual
================

Getting set up
--------------

If you want to contribute, we recommend you fork our `Github repository
<https://github.com/xnd-project/rbc>`_, then create a branch representing
your work.  When your work is ready, you should submit it as a pull
request from the Github interface.

.. _buildenv:

Build environment
'''''''''''''''''

RBC heavily depends on Numba (`Numba <https://numba.pydata.org/>`_ and
`llvmlite <https://github.com/numba/llvmlite>`_). Unless you want to
build those dependencies yourself, we recommend you use
`conda <http://conda.pydata.org/miniconda.html>`_ or
`mamba <https://mamba.readthedocs.io/en/latest/>`_
to create a dedicated development environment and install precompiled versions
of those dependencies there. Read more about the Numba dependencies here:
`numba source install <https://numba.readthedocs.io/en/stable/user/installing.html>_`.

When working with a source checkout of Numba you will also need a development
build of llvmlite. These are available from the ``numba/label/dev`` channel on
`anaconda.org <https://anaconda.org/numba/llvmlite>`_.

Then, to create an environment with a few of the most common dependencies::

   $ cd /path/to/rbc
   $ # replace "mamba" by "conda" if preferred
   $ mamba env create --file environment.yml --name rbc

.. note::
   This installs an environment based on latest Python version, but you can
   of course choose another version supported by Numba.

To activate the environment for the current shell session::

   $ conda activate rbc

.. note::
   These instructions are for a standard Linux shell.  You may need to
   adapt them for other platforms.

Once the environment is activated, you have a dedicated Python with the
required dependencies::

    .. code-block:: bash
    $ Python 3.10.8 | packaged by conda-forge | (main, Nov 22 2022, 08:26:04) [GCC 10.4.0] on linux
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import llvmlite
    >>> llvmlite.__version__
    '0.39.1'
    >>> import numba
    >>> numba.__version__
    '0.56.4'
    >>> import rbc
    >>> rbc.__version__
    '0.8.1+4.g4f73a8a'


Building Numba
''''''''''''''

For a convenient development workflow, we recommend you build Numba inside
its source checkout::

   $ git clone git@github.com:numba/numba.git
   $ cd numba
   $ python setup.py build_ext --inplace

This assumes you have a working C compiler and runtime on your development
system.  You will have to run this command again whenever you modify
C files inside the Numba source tree.

The ``build_ext`` command in Numba's setup also accepts the following
arguments:

- ``--noopt``: This disables optimization when compiling Numba's CPython
  extensions, which makes debugging them much easier. Recommended in
  conjunction with the standard ``build_ext`` option ``--debug``.
- ``--werror``: Compiles Numba's CPython extensions with the ``-Werror`` flag.
- ``--wall``: Compiles Numba's CPython extensions with the ``-Wall`` flag.

Note that Numba's CI and the conda recipe for Linux build with the ``--werror``
and ``--wall`` flags, so any contributions that change the CPython extensions
should be tested with these flags too.

.. _running-tests:

Running tests
'''''''''''''

Numba is validated using a test suite comprised of various kind of tests
(unit tests, functional tests). The test suite is written using the
standard :py:mod:`unittest` framework.

The tests can be executed via ``python -m numba.runtests``.  If you are
running Numba from a source checkout, you can type ``./runtests.py``
as a shortcut.  Various options are supported to influence test running
and reporting.  Pass ``-h`` or ``--help`` to get a glimpse at those options.
Examples:

* to list all available tests::

    $ python -m numba.runtests -l

* to list tests from a specific (sub-)suite::

    $ python -m numba.runtests -l numba.tests.test_usecases

* to run those tests::

    $ python -m numba.runtests numba.tests.test_usecases

* to run all tests in parallel, using multiple sub-processes::

    $ python -m numba.runtests -m

* For a detailed list of all options::

    $ python -m numba.runtests -h

The numba test suite can take a long time to complete.  When you want to avoid
the long wait,  it is useful to focus on the failing tests first with the
following test runner options:

* The ``--failed-first`` option is added to capture the list of failed tests
  and to re-execute them first::

    $ python -m numba.runtests --failed-first -m -v -b

* The ``--last-failed`` option is used with ``--failed-first`` to execute
  the previously failed tests only::

    $ python -m numba.runtests --last-failed -m -v -b

When debugging, it is useful to turn on logging.  Numba logs using the
standard ``logging`` module.  One can use the standard ways (i.e.
``logging.basicConfig``) to configure the logging behavior.  To enable logging
in the test runner, there is a ``--log`` flag for convenience::

    $ python -m numba.runtests --log

To enable :ref:`runtime type-checking <type_anno_check>`, set the environment
variable ``NUMBA_USE_TYPEGUARD=1`` and use `runtests.py` from the source root
instead. For example::

    $ NUMBA_USE_TYPEGUARD=1 python runtests.py

