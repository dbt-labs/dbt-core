#!/bin/bash

# move to wherever we are so docker things work
cd "$(dirname "${BASH_SOURCE[0]}")"

set -exo pipefail

docker-compose build
docker-compose up -d hive-metastore-db hadoop
docker-compose -f util.yml run --rm util wait_for_up hive-metastore-db 5432
docker-compose run --rm hive-hiveserver ./bin/schematool -initSchema -dbType postgres
docker-compose up -d hive-metastore
docker-compose -f util.yml run --rm util wait_for_up hive-metastore 9083
docker-compose up -d presto
