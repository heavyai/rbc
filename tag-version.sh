#!/bin/bash
if [[ $# -eq 0 ]]; then
	echo 'usage: ./gh-release version'
	exit 1
fi


TAG=$1
TAG_STR='v${TAG}'

# checkout to master
echo "Checking out master"
git checkout master

# make sure all tests pass
echo "running tests..."
pytest -sv rbc/

# tag a commit
echo "Creating a new tag"
git tag -a ${TAG_STR} -m "Bumping rbc to version ${TAG_STR}"
git push origin master --tags