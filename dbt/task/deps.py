import os
import shutil
import hashlib
import six

import dbt.clients.git
import dbt.clients.system
import dbt.clients.registry as registry
from dbt.clients.yaml_helper import load_yaml_text

from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.semver import VersionSpecifier, UnboundedVersionSpecifier
from dbt.utils import AttrDict

from dbt.task.base_task import BaseTask


class Package(object):
    def __init__(self, name):
        self.name = name
        self._cached_metadata = None

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

    def resolve_version(self):
        raise NotImplementedError()

    def _fetch_metadata(self, project):
        raise NotImplementedError()

    def fetch_metadata(self, project):
        if not self._cached_metadata:
            self._cached_metadata = self._fetch_metadata(project)
        return self._cached_metadata

    def get_project_name(self, project):
        metadata = self.fetch_metadata(project)
        return metadata["name"]

    def get_installation_path(self, project):
        dest_dirname = self.get_project_name(project)
        return os.path.join(project['modules-path'], dest_dirname)


class RegistryPackage(Package):
    def __init__(self, package, version):
        super(RegistryPackage, self).__init__(package)
        self.package = package
        self._version = self._sanitize_version(version)

    @classmethod
    def _sanitize_version(cls, version):
        version = [v if isinstance(v, VersionSpecifier)
                   else VersionSpecifier.from_version_string(v)
                   for v in cls.version_to_list(version)]
        # VersionSpecifier.from_version_string will return None in case of
        # failure, so we need to iterate again to check if any failed.
        if any(not isinstance(v, VersionSpecifier) for v in version):
            dbt.exceptions.raise_dependency_error(
                'Malformed version specifier found.')
        return version or [UnboundedVersionSpecifier()]

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = self._sanitize_version(version)

    def incorporate(self, other):
        return RegistryPackage(self.package, self.version + other.version)

    def _check_in_index(self):
        index = registry.index_cached()
        if self.package not in index:
            dbt.exceptions.package_not_found(self.package)

    def resolve_version(self):
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
        return RegistryPackage(self.package, target)

    def _check_version_pinned(self):
        if len(self.version) != 1:
            dbt.exceptions.raise_dependency_error(
                'Cannot fetch metadata until the version is pinned.')

    def _fetch_metadata(self, project):
        self._check_version_pinned()
        version_string = self.version[0].to_version_string(skip_matcher=True)
        return registry.package_version(self.package, version_string)

    def install(self, project):
        self._check_version_pinned()
        version_string = self.version[0].to_version_string(skip_matcher=True)
        metadata = self.fetch_metadata(project)

        tar_path = os.path.realpath('{}/downloads/{}.{}.tar.gz'.format(
            project['modules-path'],
            self.package,
            version_string))
        dbt.clients.system.make_directory(os.path.dirname(tar_path))

        download_url = metadata.get('downloads').get('tarball')
        dbt.clients.system.download(download_url, tar_path)
        deps_path = project['modules-path']
        package_name = self.get_project_name(project)
        dbt.clients.system.untar_package(tar_path, deps_path, package_name)


class GitPackage(Package):
    def __init__(self, git, version):
        super(GitPackage, self).__init__(git)
        self.git = git
        self._checkout_name = hashlib.md5(six.b(git)).hexdigest()
        self._version = self._sanitize_version(version)

    @classmethod
    def _sanitize_version(cls, version):
        return cls.version_to_list(version) or ['master']

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        self._version = self._sanitize_version(version)

    def incorporate(self, other):
        return GitPackage(self.git, self.version + other.version)

    def resolve_version(self):
        requested = set(self.version)
        if len(requested) != 1:
            logger.error(
                'Could not resolve to a single version for Git repo %s!',
                self.git)
            logger.error('  Requested versions: %s', requested)
            raise Exception('bad')
        return GitPackage(self.git, requested.pop())

    def _checkout(self, project):
        """Performs a shallow clone of the repository into the
        dbt_modules/downloads directory. This function can be called
        repeatedly. If the project has already been checked out at this
        version, it will be a no-op. Returns the path to the checked out
        directory."""
        if len(self.version) != 1:
            dbt.exceptions.raise_dependency_error(
                'Cannot checkout repository until the version is pinned.')
        checkout_cwd = os.path.join(project['modules-path'], 'downloads')
        dir_ = dbt.clients.git.clone_and_checkout(
            self.git, checkout_cwd, branch=self.version[0],
            dirname=self._checkout_name)
        return os.path.join(checkout_cwd, dir_)

    def _fetch_metadata(self, project):
        path = self._checkout(project)
        with open(os.path.join(path, 'dbt_project.yml')) as f:
            return load_yaml_text(f.read())

    def install(self, project):
        shutil.move(self._checkout(project),
                    self.get_installation_path(project))


class LocalPackage(Package):
    def __init__(self, local):
        super(LocalPackage, self).__init__(local)
        self.local = local

    def incorporate(self, _):
        return LocalPackage(self.local)

    def resolve_version(self):
        return LocalPackage(self.local)

    def _fetch_metadata(self, project):
        with open(os.path.join(self.local, 'dbt_project.yml')) as f:
            return load_yaml_text(f.read())

    def install(self, project):
        dest_path = self.get_installation_path(project)
        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)
        shutil.copytree(self.local, dest_path)


def parse_package(dict_):
    if dict_.get('package'):
        return RegistryPackage(dict_['package'], dict_.get('version'))
    if dict_.get('git'):
        return GitPackage(dict_['git'], dict_.get('version'))
    if dict_.get('local'):
        return LocalPackage(dict_['local'])
    dbt.exceptions.raise_dependency_error(
        'Malformed package definition. Must contain package, git, or local.')


class PackageListing(AttrDict):

    def incorporate(self, package):
        if not isinstance(package, Package):
            package = parse_package(package)
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


class DepsTask(BaseTask):
    def run(self):
        if not self.project.get('packages'):
            return
        dbt.clients.system.make_directory(
            os.path.join(self.project['modules-path'], 'downloads'))
        listing = PackageListing.create(self.project['packages'])
        visited_listing = PackageListing.create([])

        while listing:
            _, package = listing.popitem()

            try:
                target_package = package.resolve_version()
            except dbt.exceptions.VersionsNotCompatibleException as e:
                new_msg = ('Version error for package {}: {}'
                           .format(package.name, e))
                six.raise_from(dbt.exceptions.DependencyException(new_msg), e)
            visited_listing.incorporate(target_package)

            target_metadata = target_package.fetch_metadata(self.project)

            sub_listing = PackageListing.create(
                target_metadata.get('packages', []))
            for _, package in sub_listing.items():
                listing.incorporate(package)

        for _, package in visited_listing.items():
            logger.info('Pulling %s', package)
            package.install(self.project)
