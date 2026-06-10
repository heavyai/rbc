# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

if [[ $# -eq 0 ]]; then
        echo 'usage: ./pypi-release version'
        exit 1
fi

TAG=$1

rm -rf dist/
git checkout "tags/v${TAG}"
python3 setup.py sdist bdist_wheel
conda install -y -c conda-forge twine
python3 -m twine upload --repository testpypi dist/*
