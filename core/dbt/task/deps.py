import os
import shutil
import hashlib
import tempfile
import six
import yaml

import dbt.utils
import dbt.deprecations
import dbt.exceptions
import dbt.clients.git
import dbt.clients.system
import dbt.clients.registry as registry

from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.semver import VersionSpecifier, UnboundedVersionSpecifier
from dbt.ui import printer
from dbt.utils import AttrDict
from dbt.api.object import APIObject
from dbt.contracts.project import LOCAL_PACKAGE_CONTRACT, \
    GIT_PACKAGE_CONTRACT, REGISTRY_PACKAGE_CONTRACT, \
    REGISTRY_PACKAGE_METADATA_CONTRACT, PackageConfig

from dbt.task.base import ProjectOnlyTask

DOWNLOADS_PATH = None
REMOVE_DOWNLOADS = False
PIN_PACKAGE_URL = 'https://docs.getdbt.com/docs/package-management#section-specifying-package-versions' # noqa


def _initialize_downloads():
    global DOWNLOADS_PATH, REMOVE_DOWNLOADS
    # the user might have set an environment variable. Set it to None, and do
    # not remove it when finished.
    if DOWNLOADS_PATH is None:
        DOWNLOADS_PATH = os.environ.get('DBT_DOWNLOADS_DIR', None)
        REMOVE_DOWNLOADS = False
    # if we are making a per-run temp directory, remove it at the end of
    # successful runs
    if DOWNLOADS_PATH is None:
        DOWNLOADS_PATH = tempfile.mkdtemp(prefix='dbt-downloads-')
        REMOVE_DOWNLOADS = True

    dbt.clients.system.make_directory(DOWNLOADS_PATH)
    logger.debug("Set downloads directory='{}'".format(DOWNLOADS_PATH))


class Package(APIObject):
    SCHEMA = NotImplemented

    def __init__(self, *args, **kwargs):
        super(Package, self).__init__(*args, **kwargs)
        self._cached_metadata = None

    @property
    def name(self):
        raise NotImplementedError

    def __str__(self):
        version = getattr(self, 'version', None)
        if not version:
            return self.name
        version_str = version[0] \
            if len(version) == 1 else '<multiple versions>'
        return '{}@{}'.format(self.name, version_str)

    @classmethod
    def version_to_list(cls, version):
        if version is None:
            return []
        if not isinstance(version, (list, basestring)):
            dbt.exceptions.raise_dependency_error(
                'version must be list or string, got {}'
                .format(type(version)))
        if not isinstance(version, list):
            version = [version]
        return version

    def _resolve_version(self):
        pass

    def resolve_version(self):
        try:
            self._resolve_version()
        except dbt.exceptions.VersionsNotCompatibleException as e:
            new_msg = ('Version error for package {}: {}'
                       .format(self.name, e))
            six.raise_from(dbt.exceptions.DependencyException(new_msg), e)

    def source_type(self):
        raise NotImplementedError()

    def version_name(self):
        raise NotImplementedError()

    def nice_version_name(self):
        raise NotImplementedError()

    def _fetch_metadata(self, project):
        raise NotImplementedError()

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


class RegistryPackage(Package):
    SCHEMA = REGISTRY_PACKAGE_CONTRACT

    def __init__(self, *args, **kwargs):
        if 'version' not in kwargs:
            dbt.exceptions.raise_dependency_error(
                'package dependency {} is missing a "version" field'
                .format(kwargs.get('package'))
            )
        super(RegistryPackage, self).__init__(*args, **kwargs)
        self._version = self._sanitize_version(self._contents['version'])

    @property
    def name(self):
        return self.package

    @classmethod
    def _sanitize_version(cls, version):
        version = [v if isinstance(v, VersionSpecifier)
                   else VersionSpecifier.from_version_string(v)
                   for v in cls.version_to_list(version)]
        return version or [UnboundedVersionSpecifier()]

    def source_type(self):
        return 'hub'

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = self._sanitize_version(version)

    def version_name(self):
        self._check_version_pinned()
        version_string = self.version[0].to_version_string(skip_matcher=True)
        return version_string

    def nice_version_name(self):
        return "version {}".format(self.version_name())

    def incorporate(self, other):
        return RegistryPackage(
            package=self.package,
            version=[x.to_version_string() for x in
                     self.version + other.version]
        )

    def _check_in_index(self):
        index = registry.index_cached()
        if self.package not in index:
            dbt.exceptions.package_not_found(self.package)

    def _resolve_version(self):
        self._check_in_index()
        range_ = dbt.semver.reduce_versions(*self.version)
        available = registry.get_available_versions(self.package)
        # for now, pick a version and then recurse. later on,
        # we'll probably want to traverse multiple options
        # so we can match packages. not going to make a difference
        # right now.
        target = dbt.semver.resolve_to_specific_version(range_, available)
        if not target:
            dbt.exceptions.package_version_not_found(
                self.package, range_, available)
        self.version = target

    def _check_version_pinned(self):
        if len(self.version) != 1:
            dbt.exceptions.raise_dependency_error(
                'Cannot fetch metadata until the version is pinned.')

    def _fetch_metadata(self, project):
        version_string = self.version_name()
        dct = registry.package_version(self.package, version_string)
        return RegistryPackageMetadata(**dct)

    def install(self, project):
        version_string = self.version_name()
        metadata = self.fetch_metadata(project)

        tar_name = '{}.{}.tar.gz'.format(self.package, version_string)
        tar_path = os.path.realpath(os.path.join(DOWNLOADS_PATH, tar_name))
        dbt.clients.system.make_directory(os.path.dirname(tar_path))

        download_url = metadata['downloads']['tarball']
        dbt.clients.system.download(download_url, tar_path)
        deps_path = project.modules_path
        package_name = self.get_project_name(project)
        dbt.clients.system.untar_package(tar_path, deps_path, package_name)


# the metadata is a package config with extra attributes we don't care about.
class RegistryPackageMetadata(PackageConfig):
    SCHEMA = REGISTRY_PACKAGE_METADATA_CONTRACT


class ProjectPackageMetadata(object):
    def __init__(self, project):
        self.name = project.project_name
        self.packages = project.packages.packages


class GitPackage(Package):
    SCHEMA = GIT_PACKAGE_CONTRACT

    def __init__(self, *args, **kwargs):
        if 'warn_unpinned' in kwargs:
            kwargs['warn-unpinned'] = kwargs.pop('warn_unpinned')
        super(GitPackage, self).__init__(*args, **kwargs)
        self._checkout_name = hashlib.md5(six.b(self.git)).hexdigest()
        self.version = self._contents.get('revision')

    @property
    def other_name(self):
        if self.git.endswith('.git'):
            return self.git[:-4]
        else:
            return self.git + '.git'

    @property
    def name(self):
        return self.git

    @classmethod
    def _sanitize_version(cls, version):
        return cls.version_to_list(version) or ['master']

    def source_type(self):
        return 'git'

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = self._sanitize_version(version)

    def version_name(self):
        return self._version[0]

    def nice_version_name(self):
        return "revision {}".format(self.version_name())

    def incorporate(self, other):
        # if one is False, make both be False.
        warn_unpinned = self.warn_unpinned and other.warn_unpinned

        return GitPackage(git=self.git,
                          revision=(self.version + other.version),
                          warn_unpinned=warn_unpinned)

    def _resolve_version(self):
        requested = set(self.version)
        if len(requested) != 1:
            dbt.exceptions.raise_dependency_error(
                'git dependencies should contain exactly one version. '
                '{} contains: {}'.format(self.git, requested))
        self.version = requested.pop()

    @property
    def warn_unpinned(self):
        return self.get('warn-unpinned', True)

    def _checkout(self, project):
        """Performs a shallow clone of the repository into the downloads
        directory. This function can be called repeatedly. If the project has
        already been checked out at this version, it will be a no-op. Returns
        the path to the checked out directory."""
        if len(self.version) != 1:
            dbt.exceptions.raise_dependency_error(
                'Cannot checkout repository until the version is pinned.')
        try:
            dir_ = dbt.clients.git.clone_and_checkout(
                self.git, DOWNLOADS_PATH, branch=self.version[0],
                dirname=self._checkout_name)
        except dbt.exceptions.ExecutableError as exc:
            if exc.cmd and exc.cmd[0] == 'git':
                logger.error(
                    'Make sure git is installed on your machine. More '
                    'information: '
                    'https://docs.getdbt.com/docs/package-management'
                )
            raise
        return os.path.join(DOWNLOADS_PATH, dir_)

    def _fetch_metadata(self, project):
        path = self._checkout(project)
        if self.version[0] == 'master' and self.warn_unpinned:
            dbt.exceptions.warn_or_error(
                'The git package "{}" is not pinned.\n\tThis can introduce '
                'breaking changes into your project without warning!\n\nSee {}'
                .format(self.git, PIN_PACKAGE_URL),
                log_fmt=printer.yellow('WARNING: {}')
            )
        loaded = project.from_project_root(path, {})
        return ProjectPackageMetadata(loaded)

    def install(self, project):
        dest_path = self.get_installation_path(project)
        if os.path.exists(dest_path):
            if dbt.clients.system.path_is_symlink(dest_path):
                dbt.clients.system.remove_file(dest_path)
            else:
                dbt.clients.system.rmdir(dest_path)
        dbt.clients.system.move(self._checkout(project), dest_path)


class LocalPackage(Package):
    SCHEMA = LOCAL_PACKAGE_CONTRACT

    @property
    def name(self):
        return self.local

    def incorporate(self, _):
        return LocalPackage(local=self.local)

    def source_type(self):
        return 'local'

    def version_name(self):
        return '<local @ {}>'.format(self.local)

    def nice_version_name(self):
        return self.version_name()

    def _fetch_metadata(self, project):
        project_file_path = dbt.clients.system.resolve_path_from_base(
            self.local,
            project.project_root)

        loaded = project.from_project_root(project_file_path, {})
        return ProjectPackageMetadata(loaded)

    def install(self, project):
        src_path = dbt.clients.system.resolve_path_from_base(
            self.local,
            project.project_root)

        dest_path = self.get_installation_path(project)

        can_create_symlink = dbt.clients.system.supports_symlinks()

        if dbt.clients.system.path_exists(dest_path):
            if not dbt.clients.system.path_is_symlink(dest_path):
                dbt.clients.system.rmdir(dest_path)
            else:
                dbt.clients.system.remove_file(dest_path)

        if can_create_symlink:
            logger.debug('  Creating symlink to local dependency.')
            dbt.clients.system.make_symlink(src_path, dest_path)

        else:
            logger.debug('  Symlinks are not available on this '
                         'OS, copying dependency.')
            shutil.copytree(src_path, dest_path)


def _parse_package(dict_):
    only_1_keys = ['package', 'git', 'local']
    specified = [k for k in only_1_keys if dict_.get(k)]
    if len(specified) > 1:
        dbt.exceptions.raise_dependency_error(
            'Packages should not contain more than one of {}; '
            'yours has {} of them - {}'
            .format(only_1_keys, len(specified), specified))
    if dict_.get('package'):
        return RegistryPackage(**dict_)
    if dict_.get('git'):
        if dict_.get('version'):
            msg = ("Keyword 'version' specified for git package {}.\nDid "
                   "you mean 'revision'?".format(dict_.get('git')))
            dbt.exceptions.raise_dependency_error(msg)
        return GitPackage(**dict_)
    if dict_.get('local'):
        return LocalPackage(**dict_)
    dbt.exceptions.raise_dependency_error(
        'Malformed package definition. Must contain package, git, or local.')


class PackageListing(AttrDict):
    def __contains__(self, package):
        if isinstance(package, basestring):
            return super(PackageListing, self).__contains__(package)
        elif isinstance(package, GitPackage):
            return package.name in self or package.other_name in self
        else:
            return package.name in self

    def __setitem__(self, key, value):
        if isinstance(key, basestring):
            super(PackageListing, self).__setitem__(key, value)
        elif isinstance(key, GitPackage) and key.other_name in self:
            self[key.other_name] = value
        else:
            self[key.name] = value

    def __getitem__(self, key):
        if isinstance(key, basestring):
            return super(PackageListing, self).__getitem__(key)
        elif isinstance(key, GitPackage) and key.other_name in self:
            return self[key.other_name]
        else:
            return self[key.name]

    def incorporate(self, package):
        if not isinstance(package, Package):
            package = _parse_package(package)

        if package in self:
            self[package] = self[package].incorporate(package)
        else:
            self[package] = package

    @classmethod
    def create(cls, parsed_yaml):
        to_return = cls({})
        if not isinstance(parsed_yaml, list):
            dbt.exceptions.raise_dependency_error(
                'Package definitions must be a list, got: {}'
                .format(type(parsed_yaml)))
        for package in parsed_yaml:
            to_return.incorporate(package)
        return to_return

    def incorporate_from_yaml(self, parsed_yaml):
        listing = self.create(parsed_yaml)
        for _, package in listing.items():
            self.incorporate(package)


def _split_at_branch(repo_spec):
    parts = repo_spec.split('@')
    error = RuntimeError(
        "Invalid dep specified: '{}' -- not a repo we can clone".format(
            repo_spec
        )
    )
    repo = None
    if repo_spec.startswith('git@'):
        if len(parts) == 1:
            raise error
        if len(parts) == 2:
            repo, branch = repo_spec, None
        elif len(parts) == 3:
            repo, branch = '@'.join(parts[:2]), parts[2]
    else:
        if len(parts) == 1:
            repo, branch = parts[0], None
        elif len(parts) == 2:
            repo, branch = parts
    if repo is None:
        raise error
    return repo, branch


def _convert_repo(repo_spec):
    repo, branch = _split_at_branch(repo_spec)
    return {
        'git': repo,
        'revision': branch,
    }


def _read_packages(project_yaml):
    packages = project_yaml.get('packages', [])
    repos = project_yaml.get('repositories', [])
    if repos:
        bad_packages = [_convert_repo(r) for r in repos]
        packages += bad_packages

        fixed_packages = {"packages": bad_packages}
        recommendation = yaml.dump(fixed_packages, default_flow_style=False)
        dbt.deprecations.warn('repositories', recommendation=recommendation)
    return packages


class DepsTask(ProjectOnlyTask):
    def __init__(self, args, config=None):
        super(DepsTask, self).__init__(args=args, config=config)
        self._downloads_path = None

    @property
    def downloads_path(self):
        if self._downloads_path is None:
            self._downloads_path = tempfile.mkdtemp(prefix='dbt-downloads')
        return self._downloads_path

    def _check_for_duplicate_project_names(self, final_deps):
        seen = set()
        for _, package in final_deps.items():
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
        dbt.clients.system.make_directory(self.config.modules_path)
        _initialize_downloads()

        packages = self.config.packages.packages
        if not packages:
            logger.info('Warning: No packages were found in packages.yml')
            return

        pending_deps = PackageListing.create(packages)
        final_deps = PackageListing.create([])

        while pending_deps:
            sub_deps = PackageListing.create([])
            for package in pending_deps.values():
                final_deps.incorporate(package)
                final_deps[package].resolve_version()
                target_config = final_deps[package].fetch_metadata(self.config)
                sub_deps.incorporate_from_yaml(target_config.packages)
            pending_deps = sub_deps

        self._check_for_duplicate_project_names(final_deps)

        for package in final_deps.values():
            logger.info('Installing %s', package)
            package.install(self.config)
            logger.info('  Installed from %s\n', package.nice_version_name())

            self.track_package_install(
                package_name=package.name,
                source_type=package.source_type(),
                version=package.version_name())

        if REMOVE_DOWNLOADS:
            dbt.clients.system.rmtree(DOWNLOADS_PATH)
