import os
import errno
import re
from collections import namedtuple
import requests
import tarfile
import shutil
import hashlib
import six

import dbt.clients.git as git
import dbt.clients.system
import dbt.clients.registry as registry
import dbt.project as project
from dbt.clients.yaml_helper import load_yaml_text

from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.semver import VersionSpecifier, UnboundedVersionSpecifier
from dbt.utils import AttrDict

from dbt.task.base_task import BaseTask


class Package(object):
    def __str__(self):
        if not hasattr(self, 'version'):
            return self.name
        version_str = self.version[0] \
            if len(self.version) == 1 else '<multiple versions>'
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

    def copy(self):
        raise NotImplementedError()

    def check_against_registry_index(self, index):
        pass

    def unique_directory_name(self, prefix):
        local_digest = hashlib.md5(six.b(self.name)).hexdigest()
        return "{}--{}".format(prefix, local_digest)


class RegistryPackage(Package):
    def __init__(self, package, version):
        self.name = package
        self.package = package
        self.version = version

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

    def resolve_version(self):
        range_ = dbt.semver.reduce_versions(*self.version)
        available = registry.get_available_versions(self.package)
        # for now, pick a version and then recurse. later on,
        # we'll probably want to traverse multiple options
        # so we can match packages. not going to make a difference
        # right now.
        target = dbt.semver.resolve_to_specific_version(range_, available)
        if not target:
            logger.error(
                'Could not find a matching version for package {}!'
                .format(self.package))
            logger.error(
                '  Requested range: {}'.format(range_))
            logger.error(
                '  Available versions: {}'.format(', '.join(available)))
            raise Exception('bad')
        return RegistryPackage(self.package, target)

    def check_against_registry_index(self, index):
        if self.package not in index:
            raise Exception('unknown package {}'.format(self.package))

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

        dbt.clients.system.make_directory(
            os.path.dirname(tar_path))

        response = requests.get(version_info.get('downloads').get('tarball'))

        with open(tar_path, 'wb') as handle:
            for block in response.iter_content(1024*64):
                handle.write(block)

        with tarfile.open(tar_path, 'r') as tarball:
            tarball.extractall(project['modules-path'])


class GitPackage(Package):
    def __init__(self, git, version):
        self.name = git
        self.git = git
        self.version = version

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
        return GitPackage(self.package, self.version + other.version)

    def resolve_version(self):
        requested = set(self.version)
        if len(requested) != 1:
            logger.error(
                'Could not resolve to a single version for Git repo {}!'
                .format(self.git))
            logger.error(
                '  Requested versions: {}'.format(requested))
            raise Exception('bad')
        return GitPackage(self.git, requested.pop())

    def _checkout(self, project):
        if len(self.version) != 1:
            # error
            raise Exception('bad')
        modules_path = project['modules-path']
        dest_dirname = self.unique_directory_name("git")
        dir_ = git.clone_and_checkout(
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
        self.name = local
        self.local = local

    def incorporate(self, other):
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


def folder_from_git_remote(remote_spec):
    start = remote_spec.rfind('/') + 1
    end = len(remote_spec) - (4 if remote_spec.endswith('.git') else 0)
    return remote_spec[start:end]


class DepsTask(BaseTask):
    def __pull_repo(self, repo, branch=None):
        modules_path = self.project['modules-path']

        out, err = dbt.clients.git.clone(repo, modules_path)

        exists = re.match("fatal: destination path '(.+)' already exists",
                          err.decode('utf-8'))

        folder = None
        start_sha = None

        if exists:
            folder = exists.group(1)
            logger.info('Updating existing dependency {}.'.format(folder))
        else:
            matches = re.match("Cloning into '(.+)'", err.decode('utf-8'))
            folder = matches.group(1)
            logger.info('Pulling new dependency {}.'.format(folder))

        dependency_path = os.path.join(modules_path, folder)
        start_sha = dbt.clients.git.get_current_sha(dependency_path)
        dbt.clients.git.checkout(dependency_path, repo, branch)
        end_sha = dbt.clients.git.get_current_sha(dependency_path)

        if exists:
            if start_sha == end_sha:
                logger.info('  Already at {}, nothing to do.'.format(
                    start_sha[:7]))
            else:
                logger.info('  Updated checkout from {} to {}.'.format(
                    start_sha[:7], end_sha[:7]))
        else:
            logger.info('  Checked out at {}.'.format(end_sha[:7]))

        return folder

    def __split_at_branch(self, repo_spec):
        parts = repo_spec.split("@")
        error = RuntimeError(
            "Invalid dep specified: '{}' -- not a repo we can clone".format(
                repo_spec
            )
        )

        repo = None
        if repo_spec.startswith("git@"):
            if len(parts) == 1:
                raise error
            if len(parts) == 2:
                repo, branch = repo_spec, None
            elif len(parts) == 3:
                repo, branch = "@".join(parts[:2]), parts[2]
        else:
            if len(parts) == 1:
                repo, branch = parts[0], None
            elif len(parts) == 2:
                repo, branch = parts

        if repo is None:
            raise error

        return repo, branch

    def __pull_deps_recursive(self, repos, processed_repos=None, i=0):
        if processed_repos is None:
            processed_repos = set()
        for repo_string in repos:
            repo, branch = self.__split_at_branch(repo_string)
            repo_folder = folder_from_git_remote(repo)

            try:
                if repo_folder in processed_repos:
                    logger.info(
                        "skipping already processed dependency {}"
                        .format(repo_folder)
                    )
                else:
                    dep_folder = self.__pull_repo(repo, branch)
                    dep_project = project.read_project(
                        os.path.join(self.project['modules-path'],
                                     dep_folder,
                                     'dbt_project.yml'),
                        self.project.profiles_dir,
                        profile_to_load=self.project.profile_to_load
                    )
                    processed_repos.add(dep_folder)
                    self.__pull_deps_recursive(
                        dep_project['repositories'], processed_repos, i+1
                    )
            except IOError as e:
                if e.errno == errno.ENOENT:
                    error_string = basestring(e)

                    if 'dbt_project.yml' in error_string:
                        error_string = ("'{}' is not a valid dbt project - "
                                        "dbt_project.yml not found"
                                        .format(repo))

                    elif 'git' in error_string:
                        error_string = ("Git CLI is a dependency of dbt, but "
                                        "it is not installed!")

                    raise dbt.exceptions.RuntimeException(error_string)

                else:
                    raise e

    def run(self):
        if not self.project.get('packages'):
            return
        listing = PackageListing.create(self.project['packages'])
        visited_listing = PackageListing.create([])
        index = registry.index()

        while len(listing) > 0:
            (name, package) = listing.popitem()

            package.check_against_registry_index(index)

            target_package = package.resolve_version()
            visited_listing.incorporate(target_package)

            target_metadata = target_package.fetch_metadata(self.project)

            sub_listing = PackageListing.create(
                target_metadata.get('packages', []))
            for _, package in sub_listing.items():
                listing.incorporate(package)

        for name, package in visited_listing.items():
            logger.info('Pulling %s', package)
            package.download(self.project)
