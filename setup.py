#!/usr/bin/env python
from setuptools import setup, find_packages
import os.path

package_name = "dbt"
package_version = "0.7.1"

setup(
    name=package_name,
    version=package_version,
    description="Data build tool for Analyst Collective",
    author="Analyst Collective",
    author_email="admin@analystcollective.org",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
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
        'PyYAML>=3.11',
        'psycopg2==2.6.2',
        'sqlparse==0.1.19',
        'networkx==1.11',
        'csvkit==0.9.1',
        'snowplow-tracker==0.7.2',
        'celery==3.1.23',
        'voluptuous==0.9.3',
        'snowflake-connector-python==1.3.12',
    ],
)
