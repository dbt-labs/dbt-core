import os
import re
from typing import Dict, List, Optional

from dbt.clients import git
from dbt.config.project import PartialProject, Project
from dbt.config.renderer import PackageRenderer
from dbt.contracts.project import GitPackage, ProjectPackageMetadata
from dbt.deps.base import PinnedPackage, UnpinnedPackage, get_downloads_path
from dbt.events.types import DepsScrubbedPackageName, DepsUnpinned, EnsureGitInstalled
from dbt.exceptions import DependencyError, MultipleVersionGitDepsError
from dbt.utils import md5
from dbt_common import semver
from dbt_common.clients import system
from dbt_common.events.functions import env_secrets, fire_event, scrub_secrets
from dbt_common.exceptions import (
    ExecutableError,
    SemverError,
    VersionsNotCompatibleError,
)


def md5sum(s: str):
    return md5(s, "latin-1")


_VERSION_SPECIFIER_RE = re.compile(r"^(?P<op>>=|<=|>|<|=)?(?P<version>.+)$")


def _normalize_version_specifier(version_specifier: str) -> str:
    match = _VERSION_SPECIFIER_RE.match(version_specifier)
    if not match:
        return version_specifier
    op = match.group("op") or ""
    version = match.group("version")
    if version.startswith(("v", "V")):
        version = version[1:]
    return f"{op}{version}"


def _normalize_tag_name(tag: str) -> str:
    if tag.startswith(("v", "V")):
        return tag[1:]
    return tag


class GitPackageMixin:
    def __init__(
        self,
        git: str,
        git_unrendered: str,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.git = git
        self.git_unrendered = git_unrendered
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
        git_unrendered: str,
        revision: str,
        warn_unpinned: bool = True,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(git, git_unrendered, subdirectory)
        self.revision = revision
        self.warn_unpinned = warn_unpinned
        self.subdirectory = subdirectory
        self._checkout_name = md5sum(self.name)

    def to_dict(self) -> Dict[str, str]:
        git_scrubbed = scrub_secrets(self.git_unrendered, env_secrets())
        if self.git_unrendered != git_scrubbed:
            fire_event(
                DepsScrubbedPackageName(package_name=git_scrubbed),
                force_warn_or_error_handling=True,
            )
        ret = {
            "git": git_scrubbed,
            "revision": self.revision,
        }
        if self.subdirectory:
            ret["subdirectory"] = self.subdirectory
        return ret

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

        # raise warning (or error) if this package is not pinned
        if (self.revision == "HEAD" or self.revision in ("main", "master")) and self.warn_unpinned:
            fire_event(
                DepsUnpinned(revision=self.revision, git=self.git),
                force_warn_or_error_handling=True,
            )

        # now overwrite 'revision' with actual commit SHA
        self.revision = git.get_current_sha(path)

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
        git_unrendered: str,
        revisions: List[str],
        revision_ranges: Optional[List[str]] = None,
        warn_unpinned: bool = True,
        subdirectory: Optional[str] = None,
    ) -> None:
        super().__init__(git, git_unrendered, subdirectory)
        self.revisions = revisions
        self.revision_ranges = revision_ranges or []
        self.warn_unpinned = warn_unpinned
        self.subdirectory = subdirectory

    @classmethod
    def from_contract(cls, contract: GitPackage) -> "GitUnpinnedPackage":
        revisions = contract.get_revisions()
        revision_ranges = contract.get_revision_ranges()

        # we want to map None -> True
        warn_unpinned = contract.warn_unpinned is not False
        return cls(
            git=contract.git,
            git_unrendered=(contract.unrendered.get("git") or contract.git),
            revisions=revisions,
            revision_ranges=revision_ranges,
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
            git_unrendered=self.git_unrendered,
            revisions=self.revisions + other.revisions,
            revision_ranges=self.revision_ranges + other.revision_ranges,
            warn_unpinned=warn_unpinned,
            subdirectory=self.subdirectory,
        )

    def _parse_revision_ranges(self) -> List[semver.VersionSpecifier]:
        try:
            return [
                semver.VersionSpecifier.from_version_string(_normalize_version_specifier(value))
                for value in self.revision_ranges
            ]
        except SemverError as exc:
            raise DependencyError(
                f"Invalid revision_range for git package {self.name}: {exc}"
            ) from exc

    def _reduce_revision_ranges(self) -> semver.VersionRange:
        parsed_ranges = self._parse_revision_ranges()
        try:
            return semver.reduce_versions(*parsed_ranges)
        except VersionsNotCompatibleError as exc:
            raise DependencyError(
                f"Revision range error for git package {self.name}: {exc}"
            ) from exc

    def _resolve_revision_range(self) -> str:
        range_spec = self._reduce_revision_ranges()
        tags = git.list_remote_tags(self.git, os.getcwd())
        candidates = {}
        for tag in tags:
            normalized = _normalize_tag_name(tag)
            try:
                semver.VersionSpecifier.from_version_string(normalized)
            except SemverError:
                continue
            candidates[normalized] = tag

        if not candidates:
            raise DependencyError(f"No semantic tags found for git package {self.name}.")

        target = semver.resolve_to_specific_version(range_spec, list(candidates.keys()))
        if not target:
            available = sorted(candidates.keys())
            raise DependencyError(
                f"Could not find a matching semantic tag for git package {self.name} "
                f"within revision_range {self.revision_ranges}. Available tags: {available}"
            )

        return candidates[target]

    def _exact_revision_matches_range(
        self, revision: str, range_spec: semver.VersionRange
    ) -> bool:
        normalized = _normalize_tag_name(revision)
        try:
            semver.VersionSpecifier.from_version_string(normalized)
        except SemverError:
            return False
        return bool(semver.find_possible_versions(range_spec, [normalized]))

    def resolved(self) -> GitPinnedPackage:
        requested = set(self.revisions)
        if len(requested) > 1:
            raise MultipleVersionGitDepsError(self.name, requested)

        if requested and self.revision_ranges:
            exact_revision = requested.pop()
            range_spec = self._reduce_revision_ranges()
            if not self._exact_revision_matches_range(exact_revision, range_spec):
                raise DependencyError(
                    f"Git package {self.name} revision '{exact_revision}' does not satisfy "
                    f"revision_range {self.revision_ranges}."
                )
            return GitPinnedPackage(
                git=self.git,
                git_unrendered=self.git_unrendered,
                revision=exact_revision,
                warn_unpinned=self.warn_unpinned,
                subdirectory=self.subdirectory,
            )

        if self.revision_ranges:
            revision = self._resolve_revision_range()
        elif len(requested) == 0:
            revision = "HEAD"
        else:
            revision = requested.pop()

        return GitPinnedPackage(
            git=self.git,
            git_unrendered=self.git_unrendered,
            revision=revision,
            warn_unpinned=self.warn_unpinned,
            subdirectory=self.subdirectory,
        )
