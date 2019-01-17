#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-bigquery"
package_version = "0.13.0a1"
description = """The bigquery adapter plugin for dbt (data build tool)"""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description_content_type=description,
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/bigquery/dbt_project.yml',
            'include/bigquery/macros/*.sql',
            'include/bigquery/macros/**/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'google-cloud-bigquery>=1.0.0,<2',
    ]
)
