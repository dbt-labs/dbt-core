#!/bin/bash

. /usr/local/bin/virtualenvwrapper.sh
workon dbt

cd /usr/src/app

nosetests --with-coverage --cover-branches --cover-html --cover-html-dir=htmlcov test/unit test/integration/*
