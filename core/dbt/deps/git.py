import os
from typing import List, Optional

from dbt.clients import git, system
from dbt.config.project import PartialProject, Project
from dbt.config.renderer import PackageRenderer
from dbt.contracts.project import (
    ProjectPackageMetadata,
    GitPackage,
)
from dbt.deps.base import PinnedPackage, UnpinnedPackage, get_downloads_path
from dbt.exceptions import ExecutableError, MultipleVersionGitDepsError
from dbt.events.functions import fire_event, warn_or_error
from dbt.events.types import EnsureGitInstalled, DepsUnpinned
from dbt.utils import md5


def md5sum(s: str):
    return md5(s, "latin-1")


class GitPackageMixin:
    def __init__(
        self,
        git: str,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.git = git
        self.subdirectory = subdirectory

    @property
    def name(self):
        return f"{self.git}/{self.subdirectory}" if self.subdirectory else self.git

    def source_type(self) -> str:
        return "git"


class GitPinnedPackage(GitPackageMixin, PinnedPackage):
    def __init__(
        self,
        git: str,
        revision: str,
        warn_unpinned: bool = True,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(git, subdirectory)
        self.revision = revision
        self.warn_unpinned = warn_unpinned
        self.subdirectory = subdirectory
        self._checkout_name = md5sum(self.name)

    def get_version(self):
        return self.revision

    def get_subdirectory(self):
        return self.subdirectory

    def nice_version_name(self):
        if self.revision == "HEAD":
            return "HEAD (default revision)"
        else:
            return "revision {}".format(self.revision)

    def _checkout(self):
        """Performs a shallow clone of the repository into the downloads
        directory. This function can be called repeatedly. If the project has
        already been checked out at this version, it will be a no-op. Returns
        the path to the checked out directory."""
        try:
            dir_ = git.clone_and_checkout(
                self.git,
                get_downloads_path(),
                revision=self.revision,
                dirname=self._checkout_name,
                subdirectory=self.subdirectory,
            )
        except ExecutableError as exc:
            if exc.cmd and exc.cmd[0] == "git":
                fire_event(EnsureGitInstalled())
            raise
        return os.path.join(get_downloads_path(), dir_)

    def _fetch_metadata(
        self, project: Project, renderer: PackageRenderer
    ) -> ProjectPackageMetadata:
        path = self._checkout()

        if (self.revision == "HEAD" or self.revision in ("main", "master")) and self.warn_unpinned:
            warn_or_error(DepsUnpinned(git=self.git))
        partial = PartialProject.from_project_root(path)
        return partial.render_package_metadata(renderer)

    def install(self, project, renderer):
        dest_path = self.get_installation_path(project, renderer)
        if os.path.exists(dest_path):
            if system.path_is_symlink(dest_path):
                system.remove_file(dest_path)
            else:
                system.rmdir(dest_path)

        system.move(self._checkout(), dest_path)


class GitUnpinnedPackage(GitPackageMixin, UnpinnedPackage[GitPinnedPackage]):
    def __init__(
        self,
        git: str,
        revisions: List[str],
        warn_unpinned: bool = True,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(git, subdirectory)
        self.revisions = revisions
        self.warn_unpinned = warn_unpinned
        self.subdirectory = subdirectory

    @classmethod
    def from_contract(cls, contract: GitPackage) -> "GitUnpinnedPackage":
        revisions = contract.get_revisions()

        # we want to map None -> True
        warn_unpinned = contract.warn_unpinned is not False
        return cls(
            git=contract.git,
            revisions=revisions,
            warn_unpinned=warn_unpinned,
            subdirectory=contract.subdirectory,
        )

    def all_names(self) -> List[str]:
        if self.git.endswith(".git"):
            other = self.git[:-4]
        else:
            other = self.git + ".git"

        if self.subdirectory:
            git_name = f"{self.git}/{self.subdirectory}"
            other = f"{other}/{self.subdirectory}"
        else:
            git_name = self.git

        return [git_name, other]

    def incorporate(self, other: "GitUnpinnedPackage") -> "GitUnpinnedPackage":
        warn_unpinned = self.warn_unpinned and other.warn_unpinned

        return GitUnpinnedPackage(
            git=self.git,
            revisions=self.revisions + other.revisions,
            warn_unpinned=warn_unpinned,
            subdirectory=self.subdirectory,
        )

    def resolved(self) -> GitPinnedPackage:
        requested = set(self.revisions)
        if len(requested) == 0:
            requested = {"HEAD"}
        elif len(requested) > 1:
            raise MultipleVersionGitDepsError(self.name, requested)

        return GitPinnedPackage(
            git=self.git,
            revision=requested.pop(),
            warn_unpinned=self.warn_unpinned,
            subdirectory=self.subdirectory,
        )
