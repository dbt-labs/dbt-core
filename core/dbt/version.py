import importlib
import importlib.util
import os
import glob
import json
from typing import Iterator

import requests

import dbt.exceptions
import dbt.semver

from dbt.ui import green, red, yellow
from dbt import flags

PYPI_VERSION_URL = "https://pypi.org/pypi/dbt-core/json"


def get_latest_version(version_url: str = PYPI_VERSION_URL):
    try:
        resp = requests.get(version_url)
        data = resp.json()
        version_string = data["info"]["version"]
    except (json.JSONDecodeError, KeyError, requests.RequestException):
        return None

    return dbt.semver.VersionSpecifier.from_version_string(version_string)


def get_installed_version():
    return dbt.semver.VersionSpecifier.from_version_string(__version__)


def get_package_pypi_url(package_name: str) -> str:
    return f"https://pypi.org/pypi/dbt-{package_name}/json"


def get_version_information():
    flags.USE_COLORS = True if not flags.USE_COLORS else None

    installed = get_installed_version()
    latest = get_latest_version()

    installed_s = installed.to_version_string(skip_matcher=True)
    if latest is None:
        latest_s = "unknown"
    else:
        latest_s = latest.to_version_string(skip_matcher=True)

    version_msg = "installed version: {}\n" "   latest version: {}\n\n".format(
        installed_s, latest_s
    )

    plugin_version_msg = "Plugins:\n"
    for plugin_name, version in _get_dbt_plugins_info():
        plugin_version = dbt.semver.VersionSpecifier.from_version_string(version)
        latest_plugin_version = get_latest_version(version_url=get_package_pypi_url(plugin_name))
        plugin_update_msg = ""
        if installed == plugin_version or (
            latest_plugin_version and plugin_version == latest_plugin_version
        ):
            compatibility_msg = green("Up to date!")
        else:
            if latest_plugin_version:
                if installed.major == plugin_version.major:
                    compatibility_msg = yellow("Update available!")
                else:
                    compatibility_msg = red("Out of date!")
                plugin_update_msg = (
                    "  Your version of dbt-{} is out of date! "
                    "You can find instructions for upgrading here:\n"
                    "  https://docs.getdbt.com/dbt-cli/install/overview\n\n"
                ).format(plugin_name)
            else:
                compatibility_msg = yellow("No PYPI version available")

        plugin_version_msg += ("  - {}: {} - {}\n" "{}").format(
            plugin_name, version, compatibility_msg, plugin_update_msg
        )

    if latest is None:
        return (
            "{}The latest version of dbt could not be determined!\n"
            "Make sure that the following URL is accessible:\n{}\n\n{}".format(
                version_msg, PYPI_VERSION_URL, plugin_version_msg
            )
        )

    if installed == latest:
        return f"{version_msg}{green('Up to date!')}\n\n{plugin_version_msg}"

    elif installed > latest:
        return "{}Your version of dbt is ahead of the latest " "release!\n\n{}".format(
            version_msg, plugin_version_msg
        )

    else:
        return (
            "{}Your version of dbt is out of date! "
            "You can find instructions for upgrading here:\n"
            "https://docs.getdbt.com/docs/installation\n\n{}".format(
                version_msg, plugin_version_msg
            )
        )


def _get_adapter_plugin_names() -> Iterator[str]:
    spec = importlib.util.find_spec("dbt.adapters")
    # If None, then nothing provides an importable 'dbt.adapters', so we will
    # not be reporting plugin versions today
    if spec is None or spec.submodule_search_locations is None:
        return
    for adapters_path in spec.submodule_search_locations:
        version_glob = os.path.join(adapters_path, "*", "__version__.py")
        for version_path in glob.glob(version_glob):
            # the path is like .../dbt/adapters/{plugin_name}/__version__.py
            # except it could be \\ on windows!
            plugin_root, _ = os.path.split(version_path)
            _, plugin_name = os.path.split(plugin_root)
            yield plugin_name


def _get_dbt_plugins_info():
    for plugin_name in _get_adapter_plugin_names():
        if plugin_name == "core":
            continue
        try:
            mod = importlib.import_module(f"dbt.adapters.{plugin_name}.__version__")
        except ImportError:
            # not an adapter
            continue
        yield plugin_name, mod.version


__version__ = "1.1.0b1"
installed = get_installed_version()
