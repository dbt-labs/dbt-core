#!/usr/bin/env python
import os
import sys

if sys.version_info < (3, 6):
    print('Error: dbt does not support this version of Python.')
    print('Please upgrade to Python 3.6 or higher.')
    sys.exit(1)


from setuptools import setup
try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print('Error: dbt requires setuptools v40.1.0 or higher.')
    print('Please upgrade setuptools with "pip install --upgrade setuptools" '
          'and try again')
    sys.exit(1)


package_name = "dbt-snowflake"
package_version = "0.20.0"
description = """The snowflake adapter plugin for dbt (data build tool)"""

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    long_description_content_type='text/markdown',
    author="Fishtown Analytics",
    author_email="info@fishtownanalytics.com",
    url="https://github.com/fishtown-analytics/dbt",
    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    package_data={
        'dbt': [
            'include/snowflake/dbt_project.yml',
            'include/snowflake/sample_profiles.yml',
            'include/snowflake/macros/*.sql',
            'include/snowflake/macros/**/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'snowflake-connector-python[secure-local-storage]~=2.4.1',
        'requests<3.0.0',
        'cryptography>=3.2,<4',
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
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.6.2",
)
