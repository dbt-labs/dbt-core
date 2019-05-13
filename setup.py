#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt"
package_version = "0.13.1"
description = """With dbt, data analysts and engineers can build analytics \
the way engineers build applications."""


setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",

    packages=find_packages(),
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'dbt-postgres=={}'.format(package_version),
        'dbt-redshift=={}'.format(package_version),
        'dbt-snowflake=={}'.format(package_version),
        'dbt-bigquery=={}'.format(package_version),
    ]
)
