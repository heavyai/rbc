#!/bin/bash
if [[ $# -eq 0 ]]; then
	echo 'usage: ./update-conda-pkg version'
	exit 1
fi

if [[ ! $(which sha256sum) ]]; then
	echo "Please, install sha256sum"
	exit 1
fi

if [[ $(uname -s) == "Darwin" ]]; then
	SED_INPLACE="sed -i "" -e"
elif [[ $(uname -s) == "Linux" ]]; then
	SED_INPLACE="sed -i"
else
	echo "No windows for you!"
	exit 1
fi

TAG=$1
TAG_STR="v${TAG}"

rm -rf rbc-feedstock

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

# + Update `version` to `<tag version>` and `sha256` in `recipe.yaml`
${SED_INPLACE} "s/{% set version = \".*\" %}/{% set version = \"${TAG}\" %}/g" recipe/meta.yaml

# + Update `sha256` in `recipe.yaml`
url="https://github.com/xnd-project/rbc/archive/${TAG_STR}.tar.gz"
wget ${url}
filename="${TAG_STR}.tar.gz"
sha=$(sha256sum ${filename} | awk '{print $1;}')
${SED_INPLACE} "s/  sha256: .*/  sha256: ${sha}/g" recipe/meta.yaml

# + Update 
${SED_INPLACE} "s/  number: *./  number: 0/g" recipe/meta.yaml 

git add -u .
git commit -m "Update to ${TAG_STR}"

conda install -y -c conda-forge conda-smithy conda-forge-pinning
conda smithy rerender -c auto

git push -u origin release-${TAG_STR}

# open github and create the pull request
open "https://github.com/xnd-project/rbc-feedstock/pull/new/release-${TAG_STR}"
