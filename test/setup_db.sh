#!/bin/bash
set -x
env | grep '^PG'

# If you want to run this script for your own postgresql (run with
# docker-compose) it will look like this:
# PGHOST=127.0.0.1 PGUSER=root PGPASSWORD=password PGDATABASE=postgres \
PGUSER="${PGUSER:-postgres}"
export PGUSER
PGPORT="${PGPORT:-5432}"
export PGPORT
PGHOST="${PGHOST:-localhost}"

for i in {1..10}; do
	if pg_isready -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" ; then
		break
	fi

    echo "Waiting for postgres to be ready..."
    sleep 2;
done;

createdb dbt
psql -c "SELECT version();"
psql -c "CREATE ROLE root WITH PASSWORD 'password';"
psql -c "ALTER ROLE root WITH LOGIN;"
psql -c "GRANT CREATE, CONNECT ON DATABASE dbt TO root WITH GRANT OPTION;"

psql -c "CREATE ROLE noaccess WITH PASSWORD 'password' NOSUPERUSER;"
psql -c "ALTER ROLE noaccess WITH LOGIN;"
psql -c "GRANT CONNECT ON DATABASE dbt TO noaccess;"
psql -c "CREATE ROLE dbt_test_user_1;"
psql -c "CREATE ROLE dbt_test_user_2;"
psql -c "CREATE ROLE dbt_test_user_3;"

psql -c 'CREATE DATABASE "dbtMixedCase";'
psql -c 'GRANT CREATE, CONNECT ON DATABASE "dbtMixedCase" TO root WITH GRANT OPTION;'

set +x
