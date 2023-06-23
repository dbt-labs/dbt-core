from dbt.contracts.project import RegistryPackageMetadata, TarballPackage
from dbt.deps.base import PinnedPackage, UnpinnedPackage


class TarballPackageMixin:
    def __init__(self, tarball: str) -> None:
        super().__init__()
        self.tarball = tarball

    @property
    def name(self):
        return self.tarball

    def source_type(self) -> str:
        return "tarball"


class TarballPinnedPackage(TarballPackageMixin, PinnedPackage):
    def __init__(self, tarball: str, package: str) -> None:
        super().__init__(tarball)
        # setup to recycle RegistryPinnedPackage fns
        self.package = package
        self.version = "tarball"

    @property
    def name(self):
        return self.package

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
        package: str,
    ) -> None:
        super().__init__(tarball)
        # setup to recycle RegistryPinnedPackage fns
        self.package = package
        self.version = "tarball"

    @classmethod
    def from_contract(cls, contract: TarballPackage) -> "TarballUnpinnedPackage":
        return cls(tarball=contract.tarball, package=contract.name)

    def incorporate(self, other: "TarballUnpinnedPackage") -> "TarballUnpinnedPackage":
        return TarballUnpinnedPackage(tarball=self.tarball, package=self.package)

    def resolved(self) -> TarballPinnedPackage:
        return TarballPinnedPackage(tarball=self.tarball, package=self.package)
