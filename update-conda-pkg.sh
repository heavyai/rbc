#!/bin/bash
if [[ $# -eq 0 ]]; then
	echo 'usage: ./gh-release version'
	exit 1
fi

if [[ ! $(which sha256sum) ]]; then
	echo "Please, install sha256sum"
	exit 1
fi

TAG=$1
TAG_STR="v${TAG}"

# Update rbc-feedstock
git clone git@github.com:xnd-project/rbc-feedstock.git
cd rbc-feedstock
git remote add upstream git@github.com:conda-forge/rbc-feedstock.git
git remote add xnd-project git@github.com:xnd-project/rbc-feedstock.git
git checkout master
git fetch upstream
git reset --hard upstream/master
git push -f -u xnd-project

git branch release-${TAG_STR}
git checkout release-${TAG_STR}

# + Update ``version`` to ``<tag version>`` and ``sha256`` in ``recipe.yaml``
sed "s/{% set version = \".*\" %}/{% set version = \"${TAG}\" %}/g" recipe/meta.yaml > recipe/meta.yaml

# XXX: to-do 
# * For ``sha256``, download the tar-ball and run ``sha256sum`` on it.

git add -u .
git commit -m "Update to ${TAG_STR}"
git push -u origin release-${TAG_STR}

msg="
- Rerender: add a comment containing ``@conda-forge-admin, please rerender``

	* If it fails, run conda-smithy locally:

	  + conda install -c conda-forge conda-smithy conda-forge-pinning
	  + conda smithy rerender -c auto
	  + git push -u origin release-${TAG_STR}

- Wait until all tests pass
- Click Merge pull request
- Delete branch.
"

echo $msg

# open github and create the pull request
open https://github.com/xnd-project/rbc-feedstock/pull/new/release-${TAG_STR}
