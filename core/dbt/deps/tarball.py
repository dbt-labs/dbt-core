from typing import Dict

from dbt.contracts.project import RegistryPackageMetadata, TarballPackage
from dbt.deps.base import PinnedPackage, UnpinnedPackage
from dbt.exceptions import scrub_secrets, env_secrets
from dbt.events.functions import warn_or_error
from dbt.events.types import DepsScrubbedPackageName


class TarballPackageMixin:
    def __init__(self, tarball: str, tarball_unrendered: str) -> None:
        super().__init__()
        self.tarball = tarball
        self.tarball_unrendered = tarball_unrendered

    @property
    def name(self):
        return self.tarball

    def source_type(self) -> str:
        return "tarball"


class TarballPinnedPackage(TarballPackageMixin, PinnedPackage):
    def __init__(self, tarball: str, tarball_unrendered: str, package: str) -> None:
        super().__init__(tarball, tarball_unrendered)
        # setup to recycle RegistryPinnedPackage fns
        self.package = package
        self.version = "tarball"

    @property
    def name(self):
        return self.package

    def to_dict(self) -> Dict[str, str]:
        tarball_scrubbed = scrub_secrets(self.tarball_unrendered, env_secrets())
        if self.tarball_unrendered != tarball_scrubbed:
            warn_or_error(DepsScrubbedPackageName(package_name=tarball_scrubbed))
        return {
            "tarball": tarball_scrubbed,
            "name": self.package,
        }

    def get_version(self):
        return self.version

    def nice_version_name(self):
        return f"tarball (url: {self.tarball})"

    def _fetch_metadata(self, project, renderer):
        """
        recycle RegistryPackageMetadata so that we can use the install and
        download_and_untar from RegistryPinnedPackage next.
        build RegistryPackageMetadata from info passed via packages.yml since no
        'metadata' service exists in this case.
        """

        dct = {
            "name": self.package,
            "packages": [],  # note: required by RegistryPackageMetadata
            "downloads": {"tarball": self.tarball},
        }

        return RegistryPackageMetadata.from_dict(dct)

    def install(self, project, renderer):
        self._install(project, renderer)


class TarballUnpinnedPackage(TarballPackageMixin, UnpinnedPackage[TarballPinnedPackage]):
    def __init__(
        self,
        tarball: str,
        tarball_unrendered: str,
        package: str,
    ) -> None:
        super().__init__(tarball, tarball_unrendered)
        # setup to recycle RegistryPinnedPackage fns
        self.package = package
        self.version = "tarball"

    @classmethod
    def from_contract(cls, contract: TarballPackage) -> "TarballUnpinnedPackage":
        return cls(
            tarball=contract.tarball,
            tarball_unrendered=(contract.unrendered.get("tarball") or contract.tarball),
            package=contract.name,
        )

    def incorporate(self, other: "TarballUnpinnedPackage") -> "TarballUnpinnedPackage":
        return TarballUnpinnedPackage(
            tarball=self.tarball, tarball_unrendered=self.tarball_unrendered, package=self.package
        )

    def resolved(self) -> TarballPinnedPackage:
        return TarballPinnedPackage(
            tarball=self.tarball, tarball_unrendered=self.tarball_unrendered, package=self.package
        )
