#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-presto"
package_version = "0.13.0a1"
description = """The presto adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description_content_type=description,
    author='Fishtown Analytics',
    author_email='info@fishtownanalytics.com',
    url='https://github.com/fishtown-analytics/dbt',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/presto/dbt_project.yml',
            'include/presto/macros/*.sql',
            'include/presto/macros/*/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'presto-python-client',
    ]
)
