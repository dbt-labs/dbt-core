#!/bin/bash -eo pipefail

DBT_PATH="$( cd "$(dirname "$0")/.." ; pwd -P )"

echo $SCRIPTPATH

set -x

rm -rf "$DBT_PATH"/dist
mkdir -p "$DBT_PATH"/dist

for SUBPATH in core plugins/postgres plugins/redshift plugins/bigquery plugins/snowflake
do
    rm -rf "$DBT_PATH"/"$SUBPATH"/dist
    cd "$DBT_PATH"/"$SUBPATH"
    python setup.py sdist
    cp -r "$DBT_PATH"/"$SUBPATH"/dist/* "$DBT_PATH"/dist/
done

cd "$DBT_PATH"
python setup.py sdist

set +x
