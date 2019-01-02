#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-postgres"
package_version = "0.13.0a1"
description = """The postgres adpter plugin for dbt (data build tool)"""

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
            'include/postgres/dbt_project.yml',
            'include/postgres/macros/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'psycopg2>=2.7.5,<2.8',
    ]
)
