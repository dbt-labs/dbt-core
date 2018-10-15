import os
import shutil
import hashlib
import tempfile
import six
import yaml

import dbt.utils
import dbt.deprecations
import dbt.clients.git
import dbt.clients.system
import dbt.clients.registry as registry

from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.semver import VersionSpecifier, UnboundedVersionSpecifier
from dbt.utils import AttrDict
from dbt.api.object import APIObject
from dbt.contracts.project import LOCAL_PACKAGE_CONTRACT, \
    GIT_PACKAGE_CONTRACT, REGISTRY_PACKAGE_CONTRACT

from dbt.task.base_task import BaseTask

DOWNLOADS_PATH = os.path.join(tempfile.gettempdir(), "dbt-downloads")


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
        return metadata.project_name

    def get_installation_path(self, project):
        dest_dirname = self.get_project_name(project)
        return os.path.join(project.modules_path, dest_dirname)


class RegistryPackage(Package):
    SCHEMA = REGISTRY_PACKAGE_CONTRACT

    def __init__(self, *args, **kwargs):
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
        return RegistryPackage(self.package, self.version + other.version)

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
        # TODO(jeb): this needs to actually return a RuntimeConfig, instead of
        # parsed json from a URL
        return registry.package_version(self.package, version_string)

    def install(self, project):
        version_string = self.version_name()
        metadata = self.fetch_metadata(project)

        tar_name = '{}.{}.tar.gz'.format(self.package, version_string)
        tar_path = os.path.realpath(os.path.join(DOWNLOADS_PATH, tar_name))
        dbt.clients.system.make_directory(os.path.dirname(tar_path))

        download_url = metadata.get('downloads').get('tarball')
        dbt.clients.system.download(download_url, tar_path)
        deps_path = project.modules_path
        package_name = self.get_project_name(project)
        dbt.clients.system.untar_package(tar_path, deps_path, package_name)


class GitPackage(Package):
    SCHEMA = GIT_PACKAGE_CONTRACT

    def __init__(self, *args, **kwargs):
        super(GitPackage, self).__init__(*args, **kwargs)
        self._checkout_name = hashlib.md5(six.b(self.git)).hexdigest()
        self.version = self._contents.get('revision')

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
        return GitPackage(git=self.git,
                          revision=(self.version + other.version))

    def _resolve_version(self):
        requested = set(self.version)
        if len(requested) != 1:
            dbt.exceptions.raise_dependency_error(
                'git dependencies should contain exactly one version. '
                '{} contains: {}'.format(self.git, requested))
        self.version = requested.pop()

    def _checkout(self, project):
        """Performs a shallow clone of the repository into the downloads
        directory. This function can be called repeatedly. If the project has
        already been checked out at this version, it will be a no-op. Returns
        the path to the checked out directory."""
        if len(self.version) != 1:
            dbt.exceptions.raise_dependency_error(
                'Cannot checkout repository until the version is pinned.')
        dir_ = dbt.clients.git.clone_and_checkout(
            self.git, DOWNLOADS_PATH, branch=self.version[0],
            dirname=self._checkout_name)
        return os.path.join(DOWNLOADS_PATH, dir_)

    def _fetch_metadata(self, project):
        path = self._checkout(project)
        return project.from_project_root(path, {})

    def install(self, project):
        dest_path = self.get_installation_path(project)
        if os.path.exists(dest_path):
            if dbt.clients.system.path_is_symlink(dest_path):
                dbt.clients.system.remove_file(dest_path)
            else:
                dbt.clients.system.rmdir(dest_path)
        shutil.move(self._checkout(project), dest_path)


class LocalPackage(Package):
    SCHEMA = LOCAL_PACKAGE_CONTRACT

    @property
    def name(self):
        return self.local

    def incorporate(self, _):
        return LocalPackage(self.local)

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

        return project.from_project_root(project_file_path, {})

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

    def incorporate(self, package):
        if not isinstance(package, Package):
            package = _parse_package(package)
        if package.name not in self:
            self[package.name] = package
        else:
            self[package.name] = self[package.name].incorporate(package)

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


class DepsTask(BaseTask):
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
        dbt.clients.system.make_directory(DOWNLOADS_PATH)

        packages = self.config.packages.packages
        if not packages:
            logger.info('Warning: No packages were found in packages.yml')
            return

        pending_deps = PackageListing.create(packages)
        final_deps = PackageListing.create([])

        while pending_deps:
            sub_deps = PackageListing.create([])
            for name, package in pending_deps.items():
                final_deps.incorporate(package)
                final_deps[name].resolve_version()
                target_config = final_deps[name].fetch_metadata(self.config)
                sub_deps.incorporate_from_yaml(target_config.packages.packages)
            pending_deps = sub_deps

        self._check_for_duplicate_project_names(final_deps)

        for _, package in final_deps.items():
            logger.info('Installing %s', package)
            package.install(self.config)
            logger.info('  Installed from %s\n', package.nice_version_name())

            self.track_package_install(
                package_name=package.name,
                source_type=package.source_type(),
                version=package.version_name())
