#!/usr/bin/env python

"""
DEPRECATED: This setup.py is maintained for backwards compatibility only.

dbt-core now uses hatchling as its build backend (defined in pyproject.toml).
Please use `python -m build` or `pip install` directly instead of setup.py commands.

This file will be maintained indefinitely for legacy tooling support but is no
longer the primary build interface.
"""

from setuptools import setup

if __name__ == "__main__":
    setup()
