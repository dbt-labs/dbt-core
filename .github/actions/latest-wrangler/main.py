import os
from packaging.version import Version, parse
import requests
from typing import Any, Dict, List, Tuple


def main():
    package_name: str = os.environ["INPUT_PACKAGE_NAME"]
    new_version: Version = parse(os.environ["INPUT_NEW_VERSION"])
    github_token: str = os.environ["INPUT_GITHUB_TOKEN"]

    package_metadata, status_code = _package_metadata(package_name, github_token)
    _process_status_code(status_code, package_metadata["message"])
    published_versions = _published_versions(package_metadata)
    new_version_tags = _new_version_tags(new_version, published_versions)
    _register_tags(new_version_tags, package_name)


def _package_metadata(package_name: str, github_token: str) -> Tuple[Dict[Any, Any], int]:
    # get package metadata from github
    package_request = requests.get(
        f"https://api.github.com/orgs/dbt-labs/packages/container/{package_name}/versions",
        auth=("", github_token),
    )
    package_meta = package_request.json()
    status_code = package_request.status_code
    return package_meta, status_code


def _published_versions(package_meta) -> List[Version]:
    return [
        parse(tag)
        for version in package_meta
        for tag in version["metadata"]["container"]["tags"]
        if "latest" not in tag
    ]


def _new_version_tags(new_version: Version, published_versions: List[Version]) -> List[str]:
    # the package version is always a tag
    tags = [str(new_version)]

    # pre-releases don't get tagged with `latest`
    if new_version.is_prerelease:
        return tags

    if new_version > max(published_versions):
        tags.append("latest")

    published_patches = [
        version
        for version in published_versions
        if version.major == new_version.major and version.minor == new_version.minor
    ]
    if new_version > max(published_patches):
        tags.append(f"{new_version.major}.{new_version.minor}.latest")

    return tags


def _register_tags(tags: List[str], package_name: str) -> None:
    fully_qualified_tags = ",".join([f"ghcr.io/dbt-labs/{package_name}:{tag}" for tag in tags])
    github_output = os.environ.get("GITHUB_OUTPUT")
    with open(github_output, "at", encoding="utf-8") as gh_output:
        print(f"Registering {fully_qualified_tags}")
        gh_output.write(f"fully_qualified_tags={fully_qualified_tags}")


def _process_status_code(status_code: int, message: str) -> None:
    if status_code != 200:
        print(f"Call to GH API failed: {status_code} {message}")


if __name__ == "__main__":
    main()
