#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup


package_name = "dbt"
package_version = "0.8.2"

setup(
    name=package_name,
    version=package_version,
    description="data build tool",
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/global_project/dbt_project.yml',
            'include/global_project/macros/**/*.sql',
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
        'PyYAML>=3.11',
        'psycopg2==2.7.1',
        'sqlparse==0.2.3',
        'networkx==1.11',
        'csvkit==0.9.1',
        'snowplow-tracker==0.7.2',
        'celery==3.1.23',
        'voluptuous==0.10.5',
        'snowflake-connector-python==1.3.16',
        'colorama==0.3.9'
    ],
)
