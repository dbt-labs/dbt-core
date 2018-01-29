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
            # error
            raise Exception('bad')
        if not isinstance(version, list):
            version = [version]
        return version

    def resolve_version(self):
        raise NotImplementedError()

    def fetch_metadata(self, project):
        raise NotImplementedError()

    def unique_directory_name(self, prefix):
        local_digest = hashlib.md5(six.b(self.name)).hexdigest()[:8]
        basename = os.path.basename(self.name)
        return "{}--{}--{}".format(prefix, basename, local_digest)


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
            # error
            raise Exception('bad')
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

    def fetch_metadata(self, project):
        if len(self.version) != 1:
            # error
            raise Exception('bad')
        version_string = self.version[0].to_version_string(skip_matcher=True)
        return registry.package_version(self.package, version_string)

    def download(self, project):
        if len(self.version) != 1:
            # error
            raise Exception('bad')

        version_string = self.version[0].to_version_string(skip_matcher=True)
        version_info = registry.package_version(self.package, version_string)

        tar_path = os.path.realpath('{}/downloads/{}.{}.tar.gz'.format(
            project['modules-path'],
            self.package,
            version_string))
        dbt.clients.system.make_directory(os.path.dirname(tar_path))

        download_url = version_info.get('downloads').get('tarball')
        dbt.clients.system.download(download_url, tar_path)
        deps_path = project['modules-path']
        package_name = version_info['name']
        dbt.clients.system.untar_package(tar_path, deps_path, package_name)


class GitPackage(Package):
    def __init__(self, git, version):
        super(GitPackage, self).__init__(git)
        self.git = git
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
        if len(self.version) != 1:
            # error
            raise Exception('bad')
        modules_path = project['modules-path']
        dest_dirname = self.unique_directory_name('git')
        dir_ = dbt.clients.git.clone_and_checkout(
            self.git, modules_path, branch=self.version[0],
            dirname=dest_dirname)
        return os.path.join(modules_path, dir_)

    def fetch_metadata(self, project):
        path = self._checkout(project)
        with open(os.path.join(path, 'dbt_project.yml')) as f:
            return load_yaml_text(f.read())

    def download(self, project):
        self._checkout(project)


class LocalPackage(Package):
    def __init__(self, local):
        super(LocalPackage, self).__init__(local)
        self.local = local

    def incorporate(self, _):
        return LocalPackage(self.local)

    def resolve_version(self):
        return LocalPackage(self.local)

    def fetch_metadata(self, project):
        with open(os.path.join(self.local, 'dbt_project.yml')) as f:
            return load_yaml_text(f.read())

    def download(self, project):
        dest_dirname = self.unique_directory_name("local")
        dest_path = os.path.join(project['modules-path'], dest_dirname)
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
    # error
    raise Exception('bad')


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
            # error
            raise Exception('bad')

        for package in parsed_yaml:
            to_return.incorporate(package)

        return to_return


class DepsTask(BaseTask):
    def run(self):
        if not self.project.get('packages'):
            return
        dbt.clients.system.make_directory(self.project['modules-path'])
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
            package.download(self.project)
