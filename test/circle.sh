#!/bin/bash

nosetests --with-coverage --cover-branches --cover-html --cover-html-dir=htmlcov test/unit test/integration/*
