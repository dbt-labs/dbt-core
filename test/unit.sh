#!/bin/bash

pip install tox

cd /usr/src/app
tox -e unit-py27,unit-py35
