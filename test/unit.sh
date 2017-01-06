#!/bin/bash

pip install tox

cd /usr/src/app
time tox -e unit-py27,unit-py35,pep8
