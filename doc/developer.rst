
Developer Manual
================

Getting set up
--------------

If you want to contribute, we recommend you fork our `Github repository
<https://github.com/xnd-project/rbc>`_, then create a branch representing
your work.  When your work is ready, you should submit it as a pull
request from the Github interface.


Build environment
-----------------

RBC heavily depends on Numba (`Numba <https://numba.pydata.org/>`_ and
`llvmlite <https://github.com/numba/llvmlite>`_). Unless you want to
build those dependencies yourself, we recommend you use
`conda <http://conda.pydata.org/miniconda.html>`_ or
`mamba <https://mamba.readthedocs.io/en/latest/>`_
to create a dedicated development environment and install precompiled versions
of those dependencies there. Read more about the Numba dependencies
`here <https://numba.readthedocs.io/en/stable/user/installing.html>`_.

When working with a source checkout of Numba you will also need a development
build of llvmlite. These are available from the ``numba/label/dev`` channel on
`anaconda.org <https://anaconda.org/numba/llvmlite>`_.

Then, to create an environment with a few of the most common dependencies:

.. code-block:: bash

   $ cd /path/to/rbc
   $ # replace "mamba" by "conda" if preferred
   $ mamba env create --file environment.yml --name rbc

.. note::
    This installs an environment based on latest Python version, but you can
    of course choose another version supported by Numba.

To activate the environment for the current shell session:

.. code-block:: console

    $ conda activate rbc

.. note::
    These instructions are for a standard Linux shell. You may need to
    adapt them for other platforms.

Once the environment is activated, you have a dedicated Python with the
required dependencies:

.. code-block:: python

    $ python
    >>> import llvmlite
    >>> llvmlite.__version__
    '0.39.1'
    >>> import numba
    >>> numba.__version__
    '0.56.4'
    >>> import rbc
    >>> rbc.__version__
    '0.8.1+4.g4f73a8a'


Running tests
-------------

The tests can be executed via ``pytest -rA rbc/tests``. Various options are
supported to influence test running and reporting. See
`pytest <https://docs.pytest.org/>`_ docs for more information.


Building RBC reference docs
---------------------------

We currently use `sphinx <https://www.sphinx-doc.org/en/master/>`_ for
generating the API and reference documentation. In addition, building the
documentation also requires the
`pydata sphinx theme <https://pydata-sphinx-theme.readthedocs.io/en/stable/index.html>`_.
On a terminal, after creating the RBC conda environment, type the following:

.. code-block:: bash

    $ cd /path/to/rbc
    $ conda activate rbc
    $ export PYTHONPATH=.
    $ cd doc
    $ pip install sphinx pydata-sphinx-theme
    $ make html -j4  # Increase this number if needed

If it all goes well, this will generate ``build/html`` subdirectory, containing
the built documentation.
