#!/bin/bash

set -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python}

echo "$PYTHON_BIN"

set -x

rm -rf "$DBT_PATH"/dist
rm -rf "$DBT_PATH"/build
rm -rf "$DBT_PATH"/core/dist
rm -rf "$DBT_PATH"/core/build

mkdir -p "$DBT_PATH"/dist

cd "$DBT_PATH"/core
$PYTHON_BIN -m pip install --upgrade hatch
hatch build --clean

# Move built distributions to top-level dist/
mv "$DBT_PATH"/core/dist/* "$DBT_PATH"/dist/

set +x
