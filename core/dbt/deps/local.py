from pathlib import Path

from dbt.clients import system
import dbt.adapters.internal_storage.local_filesystem as local_SA
from dbt.deps.base import PinnedPackage, UnpinnedPackage
from dbt.contracts.project import (
    ProjectPackageMetadata,
    LocalPackage,
)


class LocalPackageMixin:
    def __init__(self, local: str) -> None:
        super().__init__()
        self.local = local

    @property
    def name(self):
        return self.local

    def source_type(self):
        return 'local'


class LocalPinnedPackage(LocalPackageMixin, PinnedPackage):
    def __init__(self, local: str) -> None:
        super().__init__(local)

    def get_version(self):
        return None

    def nice_version_name(self):
        return '<local @ {}>'.format(self.local)

    def resolve_path(self, project):
        return system.resolve_path_from_base(
            self.local,
            project.project_root,
        )

    def _fetch_metadata(self, project, renderer):
        loaded = project.from_project_root(
            self.resolve_path(project), renderer
        )
        return ProjectPackageMetadata.from_project(loaded)

    def install(self, project, renderer):
        src_path = Path(self.resolve_path(project))
        dest_path = self.get_installation_path(project, renderer)
        local_SA.delete(dest_path)
        src_path.rename(dest_path)
        # TODO: is it ok to remove symlinking?
        # Symlinks aren't really a thing outside of filesystems and will be hard to model in SAs


class LocalUnpinnedPackage(
    LocalPackageMixin, UnpinnedPackage[LocalPinnedPackage]
):
    @classmethod
    def from_contract(
        cls, contract: LocalPackage
    ) -> 'LocalUnpinnedPackage':
        return cls(local=contract.local)

    def incorporate(
        self, other: 'LocalUnpinnedPackage'
    ) -> 'LocalUnpinnedPackage':
        return LocalUnpinnedPackage(local=self.local)

    def resolved(self) -> LocalPinnedPackage:
        return LocalPinnedPackage(local=self.local)
