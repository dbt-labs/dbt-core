#!/bin/bash

pip install tox

cd /usr/src/app
tox -e integration-py27,integration-py35
