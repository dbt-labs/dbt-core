#!/usr/bin/env python
import os
import sys


if 'sdist' not in sys.argv:
    print('')
    print('As of v1.0.0, `pip install dbt` is no longer supported.')
    print('Instead, please use one of the following.')
    print('')
    print('**To use dbt with your specific database, platform, or query engine:**')
    print('')
    print('    pip install dbt-<adapter>')
    print('')
    print('    See full list: https://docs.getdbt.com/docs/available-adapters')
    print('')
    print('**For developers of integrations with dbt Core:**')
    print('')
    print('    pip install dbt-core')
    print('')
    print('    Be advised, dbt Core''s python API is not yet stable or documented')
    print('    (https://docs.getdbt.com/docs/running-a-dbt-project/dbt-api)')
    print('')
    print('**For the previous behavior of `pip install dbt`:**')
    print('')
    print('    pip install dbt-core dbt-postgres dbt-redshift dbt-snowflake dbt-bigquery')
    print('')
    sys.exit(1)


if sys.version_info < (3, 7):
    print('Error: dbt does not support this version of Python.')
    print('Please upgrade to Python 3.7 or higher.')
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

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt"
package_version = "1.0.0rc3"
description = """With dbt, data analysts and engineers can build analytics \
the way engineers build applications."""


setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-core",
    zip_safe=False,
    classifiers=[
        'Development Status :: 7 - Inactive',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.7",
)
