import json
import re

import requests

import dbt.exceptions
import dbt.semver


PYPI_VERSION_URL = 'https://pypi.org/pypi/dbt/json'


def get_latest_version():
    try:
        resp = requests.get(PYPI_VERSION_URL)
        data = resp.json()
        version_string = data['info']['version']
    except (json.JSONDecodeError, KeyError, requests.RequestException):
        return None

    return dbt.semver.VersionSpecifier.from_version_string(version_string)


def get_installed_version():
    return dbt.semver.VersionSpecifier.from_version_string(__version__)


def get_version_information():
    installed = get_installed_version()
    latest = get_latest_version()

    installed_s = installed.to_version_string(skip_matcher=True)
    if latest is None:
        latest_s = 'unknown'
    else:
        latest_s = latest.to_version_string(skip_matcher=True)

    version_msg = ("installed version: {}\n"
                   "   latest version: {}\n\n".format(installed_s, latest_s))

    if latest is None:
        return ("{}The latest version of dbt could not be determined!\n"
                "Make sure that the following URL is accessible:\n{}"
                .format(version_msg, PYPI_VERSION_URL))

    if installed == latest:
        return "{}Up to date!".format(version_msg)

    elif installed > latest:
        return ("{}Your version of dbt is ahead of the latest "
                "release!".format(version_msg))

    else:
        return ("{}Your version of dbt is out of date! "
                "You can find instructions for upgrading here:\n"
                "https://docs.getdbt.com/docs/installation"
                .format(version_msg))


__version__ = '0.12.2rc1'
installed = get_installed_version()
