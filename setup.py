#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


package_name = "dbt"
package_version = "0.13.0rc1"
description = """dbt (data build tool) is a command line tool that helps \
analysts and engineers transform data in their warehouse more effectively"""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
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
