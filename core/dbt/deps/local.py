import shutil

from dbt.clients import system
from dbt.deps.base import PinnedPackage, UnpinnedPackage
from dbt.contracts.project import (
    ProjectPackageMetadata,
    LocalPackage,
)
from dbt.logger import GLOBAL_LOGGER as logger


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
        src_path = self.resolve_path(project)
        dest_path = self.get_installation_path(project, renderer)

        can_create_symlink = system.supports_symlinks()

        if system.path_exists(dest_path):
            if not system.path_is_symlink(dest_path):
                system.rmdir(dest_path)
            else:
                system.remove_file(dest_path)

        if can_create_symlink:
            logger.debug('  Creating symlink to local dependency.')
            system.make_symlink(src_path, dest_path)

        else:
            logger.debug('  Symlinks are not available on this '
                         'OS, copying dependency.')
            shutil.copytree(src_path, dest_path)


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
