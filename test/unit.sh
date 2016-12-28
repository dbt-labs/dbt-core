#!/bin/bash

. /usr/local/bin/virtualenvwrapper.sh
mkdir -p ~/.virtualenv
mkvirtualenv dbt
workon dbt

cd /usr/src/app
tox -e unit-py27,unit-py35
