#!/bin/bash
set -x

if [ "${SKIP_HOMEBREW:-false}" = "false" ]; then
    brew install postgresql@16
    brew link postgresql@16 --force
    export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"

    # Start PostgreSQL using the full command instead of brew services
    pg_ctl -D /opt/homebrew/var/postgresql@16 start

    echo "Check PostgreSQL service is running"
    i=10
    COMMAND='pg_isready'
    while [ $i -gt -1 ]; do
        if [ $i == 0 ]; then
            echo "PostgreSQL service not ready, all attempts exhausted"
            exit 1
        fi
        echo "Check PostgreSQL service status"
        eval $COMMAND && break
        echo "PostgreSQL service not ready, wait 10 more sec, attempts left: $i"
        sleep 10
        ((i--))
    done

    createuser -s postgres

    env | grep '^PG'
fi

# If you want to run this script for your own postgresql (run with
# docker-compose) it will look like this:
# PGHOST=127.0.0.1 PGUSER=root PGPASSWORD=password PGDATABASE=postgres \
PGUSER="${PGUSER:-postgres}"
export PGUSER
PGPORT="${PGPORT:-5432}"
export PGPORT
PGHOST="${PGHOST:-localhost}"
export PGHOST
PGDATABASE="${PGDATABASE:-postgres}"
export PGDATABASE
: "${PGPASSWORD:=password}"
export PGPASSWORD

# Normalize localhost to IPv4 to avoid IPv6 (::1) surprises
if [ "${PGHOST}" = "localhost" ]; then
    PGHOST="127.0.0.1"
    export PGHOST
fi


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
