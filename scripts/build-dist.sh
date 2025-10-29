#!/bin/bash

set -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python}

echo "$PYTHON_BIN"

set -x

rm -rf "$DBT_PATH"/dist
rm -rf "$DBT_PATH"/build
mkdir -p "$DBT_PATH"/dist

cd "$DBT_PATH"
$PYTHON_BIN -m pip install --upgrade build
$PYTHON_BIN -m build


set +x
