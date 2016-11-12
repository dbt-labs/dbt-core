#!/bin/bash

nosetests --with-coverage --cover-html --cover-html-dir=htmlcov test/unit test/integration/*
