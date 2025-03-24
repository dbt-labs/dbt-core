#!/bin/bash

set -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python}

echo "$PYTHON_BIN"

set -x

rm -rf "$DBT_PATH"/dist
rm -rf "$DBT_PATH"/build
mkdir -p "$DBT_PATH"/dist

rm -rf "$DBT_PATH"/core/dist
rm -rf "$DBT_PATH"core/build
cd "$DBT_PATH"/core

# Install/upgrade wheel explicitly
$PYTHON_BIN -m pip install --upgrade wheel setuptools

# Build sdist first
$PYTHON_BIN setup.py sdist

# Build wheel separately with explicit options
$PYTHON_BIN setup.py bdist_wheel --universal

cp -r "$DBT_PATH"/"core"/dist/* "$DBT_PATH"/dist/

set +x
