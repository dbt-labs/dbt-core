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

# Copy License.md to core/ for inclusion in distribution (required by Apache 2.0)
# The license-files in pyproject.toml references it relative to core/
cp "$DBT_PATH"/License.md "$DBT_PATH"/core/License.md

cd "$DBT_PATH"/core
$PYTHON_BIN -m pip install --upgrade hatch
hatch build --clean

# Move built distributions to top-level dist/
mv "$DBT_PATH"/core/dist/* "$DBT_PATH"/dist/

# Clean up License.md that was copied to core/ for build
rm -f "$DBT_PATH/core/License.md"

set +x
