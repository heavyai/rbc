========
Releases
========

For the latest updates, see the `GitHub Releases page <https://github.com/xnd-project/rbc/releases>`_.

v0.2.0dev0 (January 13, 2020)
-----------------------------

- Implement TargetInfo to ensure the correctness of types bit-sizes.
- Add Windows OmnisciDB client support.
- Revised and cleaned up remote jit code.

v0.1.1dev1 (December 5, 2019)
-----------------------------

- Accept string port value

v0.1.1dev0 (October 20, 2019)
-----------------------------

- Omnisci UDTF support

v0.1.0dev2 (September 12, 2019)
--------------------------------

- Add OmnisciDB UDF array arguments support.
- Created conda-forge package rbc.
- Created pip package rbc-project.
- Renamed irtools.compile_to_IR to irtools.compile_to_LLVM, returns
  llvmlite.binding.ModuleRef instance.

v0.1.0dev1 (August 2, 2019)
---------------------------

First release.

How to make a new RBC release?
------------------------------

- Checkout master branch.

- Make sure that all tests pass.
  + ``pytest -sv rbc/``

- Remove ``dev?`` part from the ``VERSION`` string in ``setup.py``

  + ``git add -u .``
  + ``git commit -m "Prepare for release"``
  + ``git push -u origin master``

- Go to `GitHub Releases page <https://github.com/xnd-project/rbc/releases>`_ and click *Draft a new release*

  + Specify tag version using the following format: `v<major>.<minor>.<micro>dev<number>`. For instance, `v0.1.2dev0`.
  + Fill in the release title
  + Fill in the release description
  + Click *Publish release*

- Update rbc-feedstock

  + ``git clone git@github.com:xnd-project/rbc-feedstock.git``
  + ``cd rbc-feedstock``
  + ``git remote add upstream git@github.com:conda-forge/rbc-feedstock.git``
  + ``git remote add xnd-project git@github.com:xnd-project/rbc-feedstock.git``
  + ``git checkout master``
  + ``git fetch upstream``
  + ``git reset --hard upstream/master``
  + ``git push -f -u xnd-project``
  + ``git branch <username>/release-<tag version>``
  + ``git checkout <username>/release-<tag version>``
  + Update ``version`` to ``<tag version>`` and ``sha256`` in ``recipe.yaml``

    * For ``sha256``, download the tar-ball and run ``sha256sum`` on it.

  + ``git add -u .``
  + ``git commit -m "Update to <tag version>"``
  + ``git push -u origin <username>/release-<tag version>``
  + Open ``https://github.com/xnd-project/rbc-feedstock/pull/new/<username>/release-<tag version>`` and create pull request.
  + Rerender: add a comment containing ``@conda-forge-admin, please rerender``

    * If it fails, run conda-smithy locally:

      + ``conda install -c conda-forge conda-smithy conda-forge-pinning``
      + ``conda smithy rerender -c auto``
      + ``git push -u origin <username>/release-<tag version>``

  + Wait until all tests pass
  + Click Merge pull request
  + Delete branch.


- Update pypi

  + ``cd rbc``
  + ``rm -rf dist/``
  + ``python3 setup.py sdist bdist_wheel``
  + ``conda install -c conda-forge twine``
  + ``python3 -m twine upload dist/*``

- Finalize

  + ``cd rbc``
  + ``git checkout master``
  + Bump up RBC version in master, see ``VERSION`` in ``setup.py``
  + ``git add -u .``
  + ``git commit -m "Bump up version after release"``
  + ``git push -u origin master``
