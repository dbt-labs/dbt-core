#!/usr/bin/env python
import os
import sys

from setuptools import setup
try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print('Error: dbt requires setuptools v40.1.0 or higher.')
    print('Please upgrade setuptools with "pip install --upgrade setuptools" '
          'and try again')
    sys.exit(1)


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


package_name = "dbt-core"
package_version = "0.18.0rc1"
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
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    package_data={
        'dbt': [
            'include/index.html',
            'include/global_project/dbt_project.yml',
            'include/global_project/docs/*.md',
            'include/global_project/macros/*.sql',
            'include/global_project/macros/**/*.sql',
            'include/global_project/macros/**/**/*.sql',
            'py.typed',
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
        'Jinja2==2.11.2',
        'PyYAML>=3.11',
        'sqlparse>=0.2.3,<0.4',
        'networkx>=2.3,<3',
        'minimal-snowplow-tracker==0.0.2',
        'colorama>=0.3.9,<0.5',
        'agate>=1.6,<2',
        'isodate>=0.6,<0.7',
        'json-rpc>=1.12,<2',
        'werkzeug>=0.15,<0.17',
        'dataclasses==0.6;python_version<"3.7"',
        'hologram==0.0.10',
        'logbook>=1.5,<1.6',
        'typing-extensions>=3.7.4,<3.8',
        # the following are all to match snowflake-connector-python
        'requests>=2.18.0,<2.24.0',
        'idna<2.10',
        'cffi>=1.9,<1.15',
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    python_requires=">=3.6.3",
)
