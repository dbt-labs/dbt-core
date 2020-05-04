import importlib
import os
import json

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

    plugin_version_msg = "Plugins:\n"
    for plugin_name, version in _get_dbt_plugins_info():
        plugin_version_msg += '  - {plugin_name}: {version}\n'.format(
            plugin_name=plugin_name, version=version
        )
    if latest is None:
        return ("{}The latest version of dbt could not be determined!\n"
                "Make sure that the following URL is accessible:\n{}\n\n{}"
                .format(version_msg, PYPI_VERSION_URL, plugin_version_msg))

    if installed == latest:
        return "{}Up to date!\n\n{}".format(version_msg, plugin_version_msg)

    elif installed > latest:
        return ("{}Your version of dbt is ahead of the latest "
                "release!\n\n{}".format(version_msg, plugin_version_msg))

    else:
        return ("{}Your version of dbt is out of date! "
                "You can find instructions for upgrading here:\n"
                "https://docs.getdbt.com/docs/installation\n\n{}"
                .format(version_msg, plugin_version_msg))


def _get_dbt_plugins_info():
    for path in importlib.util.find_spec('dbt').submodule_search_locations:
        plugin_root, _ = os.path.split(path)
        _, plugin_name = os.path.split(plugin_root)
        if plugin_name == 'core':
            continue
        try:
            mod = importlib.import_module(
                f'dbt.adapters.{plugin_name}.__version__'
            )
        except ImportError:
            # not an adpater
            continue
        yield plugin_name, mod.version


__version__ = '0.17.0b1'
installed = get_installed_version()
