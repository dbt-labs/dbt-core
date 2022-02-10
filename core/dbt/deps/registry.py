import os
import functools
from typing import List

from dbt import semver
from dbt.clients import registry, system
from dbt.contracts.project import (
    RegistryPackageMetadata,
    RegistryPackage,
)
from dbt.deps.base import PinnedPackage, UnpinnedPackage, get_downloads_path
from dbt.exceptions import (
    package_version_not_found,
    VersionsNotCompatibleException,
    DependencyException,
    package_not_found,
)
from dbt.utils import _connection_exception_retry as connection_exception_retry


class RegistryPackageMixin:
    def __init__(self, package: str) -> None:
        super().__init__()
        self.package = package

    @property
    def name(self):
        return self.package

    def source_type(self) -> str:
        return "hub"


class RegistryPinnedPackage(RegistryPackageMixin, PinnedPackage):
    def __init__(self, package: str, version: str, version_latest: str) -> None:
        super().__init__(package)
        self.version = version
        self.version_latest = version_latest

    @property
    def name(self):
        return self.package

    def source_type(self):
        return "hub"

    def get_version(self):
        return self.version

    def get_version_latest(self):
        return self.version_latest

    def nice_version_name(self):
        return "version {}".format(self.version)

    def _fetch_metadata(self, project, renderer) -> RegistryPackageMetadata:
        dct = registry.package_version(self.package, self.version)
        return RegistryPackageMetadata.from_dict(dct)

    def install(self, project, renderer):
        metadata = self.fetch_metadata(project, renderer)

        tar_name = "{}.{}.tar.gz".format(self.package, self.version)
        tar_path = os.path.realpath(os.path.join(get_downloads_path(), tar_name))
        system.make_directory(os.path.dirname(tar_path))

        download_url = metadata.downloads.tarball
        deps_path = project.packages_install_path
        package_name = self.get_project_name(project, renderer)

        download_untar_fn = functools.partial(
            self.download_and_untar, download_url, tar_path, deps_path, package_name
        )
        connection_exception_retry(download_untar_fn, 5)

    def download_and_untar(self, download_url, tar_path, deps_path, package_name):
        """
        Sometimes the download of the files fails and we want to retry.  Sometimes the
        download appears successful but the file did not make it through as expected
        (generally due to a github incident).  Either way we want to retry downloading
        and untarring to see if we can get a success.  Call this within
        `_connection_exception_retry`
        """

        system.download(download_url, tar_path)
        system.untar_package(tar_path, deps_path, package_name)


class RegistryUnpinnedPackage(RegistryPackageMixin, UnpinnedPackage[RegistryPinnedPackage]):
    def __init__(
        self, package: str, versions: List[semver.VersionSpecifier], install_prerelease: bool
    ) -> None:
        super().__init__(package)
        self.versions = versions
        self.install_prerelease = install_prerelease

    def _check_in_index(self):
        index = registry.index_cached()
        if self.package not in index:
            package_not_found(self.package)

    @classmethod
    def from_contract(cls, contract: RegistryPackage) -> "RegistryUnpinnedPackage":
        raw_version = contract.get_versions()

        versions = [semver.VersionSpecifier.from_version_string(v) for v in raw_version]
        return cls(
            package=contract.package,
            versions=versions,
            install_prerelease=bool(contract.install_prerelease),
        )

    def incorporate(self, other: "RegistryUnpinnedPackage") -> "RegistryUnpinnedPackage":
        return RegistryUnpinnedPackage(
            package=self.package,
            install_prerelease=self.install_prerelease,
            versions=self.versions + other.versions,
        )

    def resolved(self) -> RegistryPinnedPackage:
        self._check_in_index()
        try:
            range_ = semver.reduce_versions(*self.versions)
        except VersionsNotCompatibleException as e:
            new_msg = "Version error for package {}: {}".format(self.name, e)
            raise DependencyException(new_msg) from e

        available = registry.get_available_versions(self.package)
        prerelease_version_specified = any(bool(version.prerelease) for version in self.versions)
        installable = semver.filter_installable(
            available, self.install_prerelease or prerelease_version_specified
        )
        available_latest = installable[-1]

        # for now, pick a version and then recurse. later on,
        # we'll probably want to traverse multiple options
        # so we can match packages. not going to make a difference
        # right now.
        target = semver.resolve_to_specific_version(range_, installable)
        if not target:
            package_version_not_found(self.package, range_, installable)
        return RegistryPinnedPackage(
            package=self.package, version=target, version_latest=available_latest
        )
