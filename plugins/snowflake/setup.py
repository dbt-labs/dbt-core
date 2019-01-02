#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-snowflake"
package_version = "0.13.0a1"
description = """The snowflake adapter plugin for dbt (data build tool)"""


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
            'include/snowflake/dbt_project.yml',
            'include/snowflake/macros/*.sql',
            'include/snowflake/macros/**/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'snowflake-connector-python>=1.4.9',
        'boto3>=1.6.23,<1.8.0',
        'botocore>=1.9.23,<1.11.0',
    ]
)
