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

function connect_circle() {
	# try to handle circleci/docker oddness
	let rc=1
	while [[ $rc -eq 1 ]]; do
		nc -z ${PGHOST} ${PGPORT}
		let rc=$?
	done
	if [[ $rc -ne 0 ]]; then
		echo "Fatal: Could not connect to $PGHOST"
		exit 1
	fi
}

# appveyor doesn't have 'nc', but it also doesn't have these issues
if [[ -n $CIRCLECI ]]; then
	connect_circle
fi

until pg_isready -h ${PGHOST} -p ${PGPORT} -U ${PGUSER}; do
    echo "Waiting for postgres to be ready..."
    sleep 2;
done;

set +x
