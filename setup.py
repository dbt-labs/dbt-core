#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

package_name = "dbt"
package_version = "0.10.1rc2"
description = """dbt (data build tool) is a command line tool that helps \
analysts and engineers transform data in their warehouse more effectively"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/global_project/dbt_project.yml',
            'include/global_project/macros/*.sql',
            'include/global_project/macros/**/*.sql',
            'include/global_project/macros/**/**/*.sql',
        ]
    },
    test_suite='test',
    entry_points={
        'console_scripts': [
            'dbt = dbt.main:main',
        ],
    },
    scripts=[
        'scripts/dbt',
    ],
    install_requires=[
        'Jinja2>=2.8',
        'pytz==2017.2',
        'PyYAML>=3.11',
        'psycopg2==2.7.1',
        'sqlparse==0.2.3',
        'networkx==1.11',
        'snowplow-tracker==0.7.2',
        'celery==3.1.23',
        'voluptuous==0.10.5',
        'snowflake-connector-python>=1.4.9',
        'requests>=2.18.0',
        'colorama==0.3.9',
        'google-cloud-bigquery==0.29.0',
        'agate>=1.6,<2',
        'jsonschema==2.6.0',
    ]
)
