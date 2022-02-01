import os
import sys
import requests
from distutils.util import strtobool
from typing import Union
from packaging.version import parse, Version

if __name__ == "__main__":

    # get inputs
    package = os.environ["INPUT_PACKAGE"]
    new_version = parse(os.environ["INPUT_NEW_VERSION"])
    gh_token = os.environ["INPUT_GH_TOKEN"]
    halt_on_missing = strtobool(os.environ.get("INPUT_HALT_ON_MISSING", "False"))

    # get package metadata from github
    package_request = requests.get(
        f"https://api.github.com/orgs/dbt-labs/packages/container/{package}/versions",
        auth=("", gh_token),
    )
    package_meta = package_request.json()

    # Log info if we don't get a 200
    if package_request.status_code != 200:
        print(f"Call to GH API failed: {package_request.status_code} {package_meta['message']}")

    # Make an early exit if there is no matching package in github
    if package_request.status_code == 404:
        if halt_on_missing:
            sys.exit(1)
        else:
            # everything is the latest if the package doesn't exist
            print(f"::set-output name=latest::{True}")
            print(f"::set-output name=minor_latest::{True}")
            sys.exit(0)

    # TODO: verify package meta is "correct"
    # https://github.com/dbt-labs/dbt-core/issues/4640

    # map versions and tags
    version_tag_map = {
        version["id"]: version["metadata"]["container"]["tags"] for version in package_meta
    }

    # is pre-release
    pre_rel = True if any(x in str(new_version) for x in ["a", "b", "rc"]) else False

    # semver of current latest
    for version, tags in version_tag_map.items():
        if "latest" in tags:
            # N.B. This seems counterintuitive, but we expect any version tagged
            # 'latest' to have exactly three associated tags:
            # latest, major.minor.latest, and major.minor.patch.
            # Subtracting everything that contains the string 'latest' gets us
            # the major.minor.patch which is what's needed for comparison.
            current_latest = parse([tag for tag in tags if "latest" not in tag][0])
        else:
            current_latest = False

    # semver of current_minor_latest
    for version, tags in version_tag_map.items():
        if f"{new_version.major}.{new_version.minor}.latest" in tags:
            # Similar to above, only now we expect exactly two tags:
            # major.minor.patch and major.minor.latest
            current_minor_latest = parse([tag for tag in tags if "latest" not in tag][0])
        else:
            current_minor_latest = False

    def is_latest(
        pre_rel: bool, new_version: Version, remote_latest: Union[bool, Version]
    ) -> bool:
        """Determine if a given contaier should be tagged 'latest' based on:
         - it's pre-release status
         - it's version
         - the version of a previously identified container tagged 'latest'

        :param pre_rel: Wether or not the version of the new container is a pre-release
        :param new_version: The version of the new container
        :param remote_latest: The version of the previously identified container that's
            already tagged latest or False
        """
        # is a pre-release = not latest
        if pre_rel:
            return False
        # + no latest tag found = is latest
        if not remote_latest:
            return True
        # + if remote version is lower than current = is latest, else not latest
        return True if remote_latest <= new_version else False

    latest = is_latest(pre_rel, new_version, current_latest)
    minor_latest = is_latest(pre_rel, new_version, current_minor_latest)

    print(f"::set-output name=latest::{latest}")
    print(f"::set-output name=minor_latest::{minor_latest}")
