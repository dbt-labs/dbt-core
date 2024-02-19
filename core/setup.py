#!/usr/bin/env python
import os
import sys

if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)


from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dbt-core"
package_version = "1.8.0a1"
description = """With dbt, data analysts and engineers can build analytics \
the way engineers build applications."""


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-core",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    test_suite="test",
    entry_points={
        "console_scripts": ["dbt = dbt.cli.main:cli"],
    },
    install_requires=[
        # ----
        # dbt-core uses these packages deeply, throughout the codebase, and there have been breaking changes in past patch releases (even though these are major-version-one).
        # Pin to the patch or minor version, and bump in each new minor version of dbt-core.
        "agate~=1.7.0",
        "Jinja2~=3.1.2",
        "mashumaro[msgpack]~=3.9",
        # ----
        # Legacy: This package has not been updated since 2019, and it is unused in dbt's logging system (since v1.0)
        # The dependency here will be removed along with the removal of 'legacy logging', in a future release of dbt-core
        "logbook>=1.5,<1.6",
        # ----
        # dbt-core uses these packages in standard ways. Pin to the major version, and check compatibility
        # with major versions in each new minor version of dbt-core.
        "click>=8.0.2,<9",
        "networkx>=2.3,<4",
        "requests<3.0.0",  # should match dbt-common
        # ----
        # These packages are major-version-0. Keep upper bounds on upcoming minor versions (which could have breaking changes)
        # and check compatibility / bump in each new minor version of dbt-core.
        "pathspec>=0.9,<0.12",
        "sqlparse>=0.2.3,<0.5",
        # ----
        # These are major-version-0 packages also maintained by dbt-labs. Accept patches.
        "dbt-extractor~=0.5.0",
        "minimal-snowplow-tracker~=0.0.2",
        "dbt-semantic-interfaces~=0.5.0a2",
        "dbt-common~=0.1.3",
        "dbt-adapters~=0.1.0a2",
        # ----
        # Expect compatibility with all new versions of these packages, so lower bounds only.
        "packaging>20.9",
        "protobuf>=4.0.0",
        "pytz>=2015.7",
        "pyyaml>=6.0",
        "daff>=1.3.46",
        "typing-extensions>=4.4",
        # ----
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
