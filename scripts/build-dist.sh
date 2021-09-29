#!/bin/bash 

set -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

PYTHON_BIN=${PYTHON_BIN:-python}

echo "$PYTHON_BIN"

set -x

rm -rf "$DBT_PATH"/dist
rm -rf "$DBT_PATH"/build
mkdir -p "$DBT_PATH"/dist

for SUBPATH in core plugins/postgres
do
    rm -rf "$DBT_PATH"/"$SUBPATH"/dist
    rm -rf "$DBT_PATH"/"$SUBPATH"/build
    cd "$DBT_PATH"/"$SUBPATH"
    $PYTHON_BIN setup.py sdist bdist_wheel
    cp -r "$DBT_PATH"/"$SUBPATH"/dist/* "$DBT_PATH"/dist/
done

cd "$DBT_PATH"
$PYTHON_BIN setup.py sdist bdist_wheel

set +x
