import abc
import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from typing import Union, Dict, Optional, List

import dbt.utils
import dbt.deprecations
import dbt.exceptions
from dbt import semver
from dbt.ui import printer

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.clients import git, registry, system
from dbt.contracts.project import ProjectPackageMetadata, \
    RegistryPackageMetadata, \
    LocalPackage as LocalPackageContract, \
    GitPackage as GitPackageContract, \
    RegistryPackage as RegistryPackageContract
from dbt.exceptions import raise_dependency_error, package_version_not_found, \
    VersionsNotCompatibleException, DependencyException

from dbt.task.base import ProjectOnlyTask

DOWNLOADS_PATH = None
REMOVE_DOWNLOADS = False
PIN_PACKAGE_URL = 'https://docs.getdbt.com/docs/package-management#section-specifying-package-versions' # noqa


def _initialize_downloads():
    global DOWNLOADS_PATH, REMOVE_DOWNLOADS
    # the user might have set an environment variable. Set it to None, and do
    # not remove it when finished.
    if DOWNLOADS_PATH is None:
        DOWNLOADS_PATH = os.getenv('DBT_DOWNLOADS_DIR')
        REMOVE_DOWNLOADS = False
    # if we are making a per-run temp directory, remove it at the end of
    # successful runs
    if DOWNLOADS_PATH is None:
        DOWNLOADS_PATH = tempfile.mkdtemp(prefix='dbt-downloads-')
        REMOVE_DOWNLOADS = True

    system.make_directory(DOWNLOADS_PATH)
    logger.debug("Set downloads directory='{}'".format(DOWNLOADS_PATH))


PackageContract = Union[LocalPackageContract, GitPackageContract,
                        RegistryPackageContract]


def _parse_package(dict_: dict) -> PackageContract:
    only_1_keys = ['package', 'git', 'local']
    specified = [k for k in only_1_keys if dict_.get(k)]
    if len(specified) > 1:
        dbt.exceptions.raise_dependency_error(
            'Packages should not contain more than one of {}; '
            'yours has {} of them - {}'
            .format(only_1_keys, len(specified), specified))
    if dict_.get('package'):
        return RegistryPackageContract.from_dict(dict_)
    if dict_.get('git'):
        if dict_.get('version'):
            msg = ("Keyword 'version' specified for git package {}.\nDid "
                   "you mean 'revision'?".format(dict_.get('git')))
            dbt.exceptions.raise_dependency_error(msg)
        return GitPackageContract.from_dict(dict_)
    if dict_.get('local'):
        return LocalPackageContract.from_dict(dict_)
    dbt.exceptions.raise_dependency_error(
        'Malformed package definition. Must contain package, git, or local.')


def md5sum(s: str):
    return hashlib.md5(s.encode('latin-1')).hexdigest()


@dataclass
class Pinned(metaclass=abc.ABCMeta):
    _cached_metadata: Optional[ProjectPackageMetadata] = field(init=False)

    def __post_init__(self):
        self._cached_metadata = None

    def __str__(self):
        version = self.get_version()
        if not version:
            return self.name

        return '{}@{}'.format(self.name, version)

    @abc.abstractproperty
    def name(self):
        raise NotImplementedError

    @abc.abstractmethod
    def source_type(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_version(self) -> Optional[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def _fetch_metadata(self, project):
        raise NotImplementedError

    @abc.abstractmethod
    def install(self, project):
        raise NotImplementedError

    @abc.abstractmethod
    def nice_version_name(self):
        raise NotImplementedError

    def fetch_metadata(self, project):
        if not self._cached_metadata:
            self._cached_metadata = self._fetch_metadata(project)
        return self._cached_metadata

    def get_project_name(self, project):
        metadata = self.fetch_metadata(project)
        return metadata.name

    def get_installation_path(self, project):
        dest_dirname = self.get_project_name(project)
        return os.path.join(project.modules_path, dest_dirname)


@dataclass
class LocalPinned(Pinned):
    local: str

    @property
    def name(self):
        return self.local

    def source_type(self):
        return 'local'

    def get_version(self):
        return None

    def nice_version_name(self):
        return '<local @ {}>'.format(self.local)

    def resolve_path(self, project):
        return system.resolve_path_from_base(
            self.local,
            project.project_root,
        )

    def _fetch_metadata(self, project):
        loaded = project.from_project_root(self.resolve_path(project), {})
        return ProjectPackageMetadata.from_project(loaded)

    def install(self, project):
        src_path = self.resolve_path(project)
        dest_path = self.get_installation_path(project)

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


@dataclass
class GitPinned(Pinned):
    git: str
    revision: str
    warn_unpinned: bool = True
    _checkout_name: str = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self._checkout_name = md5sum(self.git)

    @property
    def name(self):
        return self.git

    def source_type(self):
        return 'git'

    def get_version(self):
        return self.revision

    def nice_version_name(self):
        return 'revision {}'.format(self.revision)

    def _checkout(self):
        """Performs a shallow clone of the repository into the downloads
        directory. This function can be called repeatedly. If the project has
        already been checked out at this version, it will be a no-op. Returns
        the path to the checked out directory."""
        try:
            dir_ = git.clone_and_checkout(
                self.git, DOWNLOADS_PATH, branch=self.revision,
                dirname=self._checkout_name
            )
        except dbt.exceptions.ExecutableError as exc:
            if exc.cmd and exc.cmd[0] == 'git':
                logger.error(
                    'Make sure git is installed on your machine. More '
                    'information: '
                    'https://docs.getdbt.com/docs/package-management'
                )
            raise
        return os.path.join(DOWNLOADS_PATH, dir_)

    def _fetch_metadata(self, project) -> ProjectPackageMetadata:
        path = self._checkout()
        if self.revision == 'master' and self.warn_unpinned:
            dbt.exceptions.warn_or_error(
                'The git package "{}" is not pinned.\n\tThis can introduce '
                'breaking changes into your project without warning!\n\nSee {}'
                .format(self.git, PIN_PACKAGE_URL),
                log_fmt=printer.yellow('WARNING: {}')
            )
        loaded = project.from_project_root(path, {})
        return ProjectPackageMetadata.from_project(loaded)

    def install(self, project):
        dest_path = self.get_installation_path(project)
        if os.path.exists(dest_path):
            if system.path_is_symlink(dest_path):
                system.remove_file(dest_path)
            else:
                system.rmdir(dest_path)

        system.move(self._checkout(), dest_path)


@dataclass
class RegistryPinned(Pinned):
    package: str
    version: str

    @property
    def name(self):
        return self.package

    def source_type(self):
        return 'hub'

    def get_version(self):
        return self.version

    def nice_version_name(self):
        return 'version {}'.format(self.version)

    def _fetch_metadata(self, project) -> RegistryPackageMetadata:
        dct = registry.package_version(self.package, self.version)
        return RegistryPackageMetadata.from_dict(dct)

    def install(self, project):
        metadata = self.fetch_metadata(project)

        tar_name = '{}.{}.tar.gz'.format(self.package, self.version)
        tar_path = os.path.realpath(os.path.join(DOWNLOADS_PATH, tar_name))
        system.make_directory(os.path.dirname(tar_path))

        download_url = metadata.downloads.tarball
        system.download(download_url, tar_path)
        deps_path = project.modules_path
        package_name = self.get_project_name(project)
        system.untar_package(tar_path, deps_path, package_name)


class Package(metaclass=abc.ABCMeta):
    @abc.abstractclassmethod
    def from_contract(cls, contract):
        raise NotImplementedError

    @abc.abstractproperty
    def name(self):
        raise NotImplementedError

    def all_names(self):
        return [self.name]

    def _typecheck(self, other):
        if not isinstance(other, self.__class__):
            raise_dependency_error(
                'Cannot incorporate {0} ({0.__class__.__name__}) into '
                '{1} ({1.__class__.__name__}): mismatched types'
                .format(other, self))


@dataclass
class LocalPackage(Package):
    local: str

    def source_type(self):
        return 'local'

    @property
    def name(self):
        return self.local

    @classmethod
    def from_contract(cls, contract: LocalPackageContract) -> 'LocalPackage':
        return cls(local=contract.local)

    def incorporate(
        self, other: Union['LocalPackage', LocalPinned]
    ) -> 'LocalPackage':
        if isinstance(other, LocalPinned):
            other = LocalPackage(local=other.local)
        self._typecheck(other)
        return LocalPackage(local=self.local)

    def resolved(self) -> LocalPinned:
        return LocalPinned(local=self.local)


@dataclass
class GitPackage(Package):
    git: str
    revisions: List[str]
    warn_unpinned: bool = True

    @classmethod
    def from_contract(cls, contract: GitPackageContract) -> 'GitPackage':
        revisions = [contract.revision] if contract.revision else []

        # we want to map None -> True
        warn_unpinned = contract.warn_unpinned is not False
        return cls(git=contract.git, revisions=revisions,
                   warn_unpinned=warn_unpinned)

    @property
    def name(self):
        return self.git

    def source_type(self):
        return 'git'

    def all_names(self):
        if self.git.endswith('.git'):
            other = self.git[:-4]
        else:
            other = self.git + '.git'
        return [self.git, other]

    def incorporate(
        self, other: Union['GitPackage', GitPinned]
    ) -> 'GitPackage':

        if isinstance(other, GitPinned):
            other = GitPackage(git=other.git, revisions=[other.revision],
                               warn_unpinned=other.warn_unpinned)

        self._typecheck(other)

        warn_unpinned = self.warn_unpinned and other.warn_unpinned

        return GitPackage(git=self.git,
                          revisions=self.revisions + other.revisions,
                          warn_unpinned=warn_unpinned)

    def resolved(self) -> GitPinned:
        requested = set(self.revisions)
        if len(requested) == 0:
            requested = {'master'}
        elif len(requested) > 1:
            dbt.exceptions.raise_dependency_error(
                'git dependencies should contain exactly one version. '
                '{} contains: {}'.format(self.git, requested))

        return GitPinned(
            git=self.git, revision=requested.pop(),
            warn_unpinned=self.warn_unpinned
        )


@dataclass
class RegistryPackage(Package):
    package: str
    versions: List[semver.VersionSpecifier]

    @property
    def name(self):
        return self.package

    def _check_in_index(self):
        index = registry.index_cached()
        if self.package not in index:
            dbt.exceptions.package_not_found(self.package)

    @classmethod
    def from_contract(
        cls, contract: RegistryPackageContract
    ) -> 'RegistryPackage':
        raw_version = contract.version
        if isinstance(raw_version, str):
            raw_version = [raw_version]

        versions = [
            semver.VersionSpecifier.from_version_string(v)
            for v in raw_version
        ]
        return cls(package=contract.package, versions=versions)

    def incorporate(
        self, other: ['RegistryPackage', RegistryPinned]
    ) -> 'RegistryPackage':
        if isinstance(other, RegistryPinned):
            versions = [
                semver.VersionSpecifier.from_version_string(other.version)
            ]
            other = RegistryPackage(package=other.package, versions=versions)
        self._typecheck(other)
        return RegistryPackage(package=self.package,
                               versions=self.versions + other.versions)

    def resolved(self) -> RegistryPinned:
        self._check_in_index()
        try:
            range_ = semver.reduce_versions(*self.versions)
        except VersionsNotCompatibleException as e:
            new_msg = ('Version error for package {}: {}'
                       .format(self.name, e))
            raise DependencyException(new_msg) from e

        available = registry.get_available_versions(self.package)

        # for now, pick a version and then recurse. later on,
        # we'll probably want to traverse multiple options
        # so we can match packages. not going to make a difference
        # right now.
        target = semver.resolve_to_specific_version(range_, available)
        if not target:
            package_version_not_found(self.package, range_, available)
        return RegistryPinned(package=self.package, version=target)


PackageResolver = Union[LocalPackage, GitPackage, RegistryPackage]
PinnedPackages = Union[LocalPinned, GitPinned, RegistryPinned]


@dataclass
class PackageListing:
    packages: Dict[str, PackageResolver] = field(default_factory=dict)

    def __len__(self):
        return len(self.packages)

    def __bool__(self):
        return bool(self.packages)

    def _pick_key(self, key: Package):
        for name in key.all_names():
            if name in self.packages:
                return name
        return key.name

    def __contains__(self, key: Package):
        for name in key.all_names():
            if name in self.packages:
                return True

    def __getitem__(self, key: Package):
        key = self._pick_key(key)
        return self.packages[key]

    def __setitem__(self, key: Package, value):
        key = self._pick_key(key)
        self.packages[key] = value

    def incorporate(self, package: Package):
        key = self._pick_key(package)
        if key in self.packages:
            self.packages[key] = self.packages[key].incorporate(package)
        else:
            self.packages[key] = package

    def update_from(self, src: List[PackageContract]) -> 'PackageListing':
        for contract in src:
            if isinstance(contract, LocalPackageContract):
                pkg = LocalPackage.from_contract(contract)
            elif isinstance(contract, GitPackageContract):
                pkg = GitPackage.from_contract(contract)
            elif isinstance(contract, RegistryPackageContract):
                pkg = RegistryPackage.from_contract(contract)
            else:
                raise dbt.exceptions.InternalException(
                    'Invalid package type {}'.format(type(contract))
                )
            self.incorporate(pkg)

    @classmethod
    def from_contracts(
        cls: 'PackageListing', src: List[PackageContract]
    ) -> 'PackageListing':
        self = cls({})
        self.update_from(src)
        return self

    def resolved(self) -> List[PinnedPackages]:
        return [p.resolved() for p in self.packages.values()]

    def __iter__(self):
        return iter(self.packages.values())


def resolve_packages(
    packages: List[PackageContract], config
) -> List[PinnedPackages]:
    pending = PackageListing.from_contracts(packages)
    final = PackageListing()

    while pending:
        next_pending = PackageListing()
        # resolve the dependency in question
        for package in pending:
            final.incorporate(package)
            target = final[package].resolved().fetch_metadata(config)
            next_pending.update_from(target.packages)
        pending = next_pending
    return final.resolved()


class DepsTask(ProjectOnlyTask):
    def __init__(self, args, config=None):
        super().__init__(args=args, config=config)
        self._downloads_path = None

    @property
    def downloads_path(self):
        if self._downloads_path is None:
            self._downloads_path = tempfile.mkdtemp(prefix='dbt-downloads')
        return self._downloads_path

    def _check_for_duplicate_project_names(self, final_deps):
        seen = set()
        for package in final_deps:
            project_name = package.get_project_name(self.config)
            if project_name in seen:
                dbt.exceptions.raise_dependency_error(
                    'Found duplicate project {}. This occurs when a dependency'
                    ' has the same project name as some other dependency.'
                    .format(project_name))
            seen.add(project_name)

    def track_package_install(self, package_name, source_type, version):
        version = 'local' if source_type == 'local' else version

        h_package_name = dbt.utils.md5(package_name)
        h_version = dbt.utils.md5(version)

        dbt.tracking.track_package_install({
            "name": h_package_name,
            "source": source_type,
            "version": h_version
        })

    def run(self):
        system.make_directory(self.config.modules_path)
        _initialize_downloads()

        packages = self.config.packages.packages
        if not packages:
            logger.info('Warning: No packages were found in packages.yml')
            return

        final_deps = resolve_packages(packages, self.config)

        self._check_for_duplicate_project_names(final_deps)

        for package in final_deps:
            logger.info('Installing {}', package)
            package.install(self.config)
            logger.info('  Installed from {}\n', package.nice_version_name())

            self.track_package_install(
                package_name=package.name,
                source_type=package.source_type(),
                version=package.get_version())

        if REMOVE_DOWNLOADS:
            system.rmtree(DOWNLOADS_PATH)
