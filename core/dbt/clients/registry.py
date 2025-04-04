import functools
import os
from typing import Any, Dict, List, Optional

import requests

from dbt import deprecations
from dbt.events.types import (
    RegistryIndexProgressGETRequest,
    RegistryIndexProgressGETResponse,
    RegistryProgressGETRequest,
    RegistryProgressGETResponse,
    RegistryResponseExtraNestedKeys,
    RegistryResponseMissingNestedKeys,
    RegistryResponseMissingTopKeys,
    RegistryResponseUnexpectedType,
)
from dbt.utils import memoized
from dbt_common import semver
from dbt_common.events.functions import fire_event
from dbt_common.utils.connection import connection_exception_retry

if os.getenv("DBT_PACKAGE_HUB_URL"):
    DEFAULT_REGISTRY_BASE_URL = os.getenv("DBT_PACKAGE_HUB_URL")
else:
    DEFAULT_REGISTRY_BASE_URL = "https://hub.getdbt.com/"


def _get_url(name, registry_base_url=None):
    if registry_base_url is None:
        registry_base_url = DEFAULT_REGISTRY_BASE_URL
    url = "api/v1/{}.json".format(name)

    return "{}{}".format(registry_base_url, url)


def _check_package_redirect(package_name: str, response: Dict[str, Any]) -> None:
    # Either redirectnamespace or redirectname in the JSON response indicate a redirect
    # redirectnamespace redirects based on package ownership
    # redirectname redirects based on package name
    # Both can be present at the same time, or neither. Fails gracefully to old name
    if ("redirectnamespace" in response) or ("redirectname" in response):
        use_namespace = response.get("redirectnamespace") or response["namespace"]
        use_name = response.get("redirectname") or response["name"]

        new_nwo = f"{use_namespace}/{use_name}"
        deprecations.warn("package-redirect", old_name=package_name, new_name=new_nwo)


def _get_package_with_retries(
    package_name: str, registry_base_url: Optional[str] = None
) -> Dict[str, Any]:
    get_fn = functools.partial(_get, package_name, registry_base_url)
    response: Dict[str, Any] = connection_exception_retry(get_fn, 5)
    _check_package_redirect(package_name, response)
    return response


def _get(package_name, registry_base_url=None):
    url = _get_url(package_name, registry_base_url)
    fire_event(RegistryProgressGETRequest(url=url))
    # all exceptions from requests get caught in the retry logic so no need to wrap this here
    resp = requests.get(url, timeout=30)
    fire_event(RegistryProgressGETResponse(url=url, resp_code=resp.status_code))
    resp.raise_for_status()

    # The response should always be a dictionary.  Anything else is unexpected, raise error.
    # Raising this error will cause this function to retry (if called within _get_package_with_retries)
    # and hopefully get a valid response.  This seems to happen when there's an issue with the Hub.
    # Since we control what we expect the HUB to return, this is safe.
    # See https://github.com/dbt-labs/dbt-core/issues/4577
    # and https://github.com/dbt-labs/dbt-core/issues/4849
    response = resp.json()

    if not isinstance(response, dict):  # This will also catch Nonetype
        error_msg = (
            f"Request error: Expected a response type of <dict> but got {type(response)} instead"
        )
        fire_event(RegistryResponseUnexpectedType(response=response))
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    # check for expected top level keys
    expected_keys = {"name", "versions"}
    if not expected_keys.issubset(response):
        error_msg = (
            f"Request error: Expected the response to contain keys {expected_keys} "
            f"but is missing {expected_keys.difference(set(response))}"
        )
        fire_event(RegistryResponseMissingTopKeys(response=response))
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    # check for the keys we need nested under each version
    expected_version_keys = {"name", "packages", "downloads"}
    all_keys = set().union(*(response["versions"][d] for d in response["versions"]))
    if not expected_version_keys.issubset(all_keys):
        error_msg = (
            "Request error: Expected the response for the version to contain keys "
            f"{expected_version_keys} but is missing {expected_version_keys.difference(all_keys)}"
        )
        fire_event(RegistryResponseMissingNestedKeys(response=response))
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    # all version responses should contain identical keys.
    has_extra_keys = set().difference(*(response["versions"][d] for d in response["versions"]))
    if has_extra_keys:
        error_msg = (
            "Request error: Keys for all versions do not match.  Found extra key(s) "
            f"of {has_extra_keys}."
        )
        fire_event(RegistryResponseExtraNestedKeys(response=response))
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    return response


_get_cached = memoized(_get_package_with_retries)


def package(package_name, registry_base_url=None) -> Dict[str, Any]:
    # returns a dictionary of metadata for all versions of a package
    response = _get_cached(package_name, registry_base_url)
    return response["versions"]


def package_version(package_name, version, registry_base_url=None) -> Dict[str, Any]:
    # returns the metadata of a specific version of a package
    response = package(package_name, registry_base_url)
    return response[version]


def is_compatible_version(package_spec, dbt_version) -> bool:
    require_dbt_version = package_spec.get("require_dbt_version")
    if not require_dbt_version:
        # if version requirements are missing or empty, assume any version is compatible
        return True
    else:
        # determine whether dbt_version satisfies this package's require-dbt-version config
        if not isinstance(require_dbt_version, list):
            require_dbt_version = [require_dbt_version]
        supported_versions = [
            semver.VersionSpecifier.from_version_string(v) for v in require_dbt_version
        ]
        return semver.versions_compatible(dbt_version, *supported_versions)


def get_compatible_versions(package_name, dbt_version, should_version_check) -> List["str"]:
    # returns a list of all available versions of a package
    response = package(package_name)

    # if the user doesn't care about installing compatible versions, just return them all
    if not should_version_check:
        return list(response)

    # otherwise, only return versions that are compatible with the installed version of dbt-core
    else:
        compatible_versions = [
            pkg_version
            for pkg_version, info in response.items()
            if is_compatible_version(info, dbt_version)
        ]
        return compatible_versions


def _get_index(registry_base_url=None):
    url = _get_url("index", registry_base_url)
    fire_event(RegistryIndexProgressGETRequest(url=url))
    # all exceptions from requests get caught in the retry logic so no need to wrap this here
    resp = requests.get(url, timeout=30)
    fire_event(RegistryIndexProgressGETResponse(url=url, resp_code=resp.status_code))
    resp.raise_for_status()

    # The response should be a list.  Anything else is unexpected, raise an error.
    # Raising this error will cause this function to retry and hopefully get a valid response.

    response = resp.json()

    if not isinstance(response, list):  # This will also catch Nonetype
        error_msg = (
            f"Request error: The response type of {type(response)} is not valid: {resp.text}"
        )
        raise requests.exceptions.ContentDecodingError(error_msg, response=resp)

    return response


def index(registry_base_url=None) -> List[str]:
    # this returns a list of all packages on the Hub
    get_index_fn = functools.partial(_get_index, registry_base_url)
    return connection_exception_retry(get_index_fn, 5)


index_cached = memoized(index)
