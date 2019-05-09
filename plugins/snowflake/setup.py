#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-snowflake"
package_version = "0.13.1a2"
description = """The snowflake adapter plugin for dbt (data build tool)"""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/snowflake/dbt_project.yml',
            'include/snowflake/macros/*.sql',
            'include/snowflake/macros/**/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'snowflake-connector-python>=1.6.12',
    ]
)
