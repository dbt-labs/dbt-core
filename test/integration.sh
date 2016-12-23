#!/bin/bash

. /usr/src/app/test/setup.sh
workon dbt

cd /usr/src/app
tox -e integration-py27,integration-py35
