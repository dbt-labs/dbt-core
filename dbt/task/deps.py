import os
import errno
import re

import dbt.clients.git
import dbt.clients.system
import dbt.clients.registry
import dbt.project as project

from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.semver import VersionSpecifier, UnboundedVersionSpecifier
from dbt.utils import AttrDict

from dbt.task.base_task import BaseTask


class PackageListing(AttrDict):

    @classmethod
    def convert_version_strings(cls, version_strings):
        if not isinstance(version_strings, list):
            version_strings = [version_strings]

        return [
            VersionSpecifier.from_version_string(version_string)
            for version_string in version_strings
        ]

    def incorporate(self, package, version_specifiers=None):
        if version_specifiers is None:
            version_specifiers = [UnboundedVersionSpecifier()]
        elif not isinstance(version_specifiers, list):
            # error
            raise Exception('bad')
        else:
            for version_specifier in version_specifiers:
                if not isinstance(version_specifier, VersionSpecifier):
                    # error
                    raise Exception('bad')

        if package not in self:
            self[package] = version_specifiers

        else:
            self[package] = self[package] + version_specifiers

    @classmethod
    def create(cls, parsed_yaml):
        to_return = cls({})

        if not isinstance(parsed_yaml, list):
            # error
            raise Exception('bad')

        if isinstance(parsed_yaml, list):
            for package in parsed_yaml:
                if isinstance(package, basestring):
                    to_return.incorporate(package)
                elif isinstance(package, dict):
                    (package, version_strings) = package.popitem()
                    to_return.incorporate(
                        package,
                        cls.convert_version_strings(version_strings))

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
        listing = PackageListing.create(self.project.get('packages', []))
        visited_listing = self.get_required_listing(listing)
        self.fetch_required_packages(visited_listing)

    def get_required_listing(self, listing):
        visited_listing = PackageListing.create([])
        index = dbt.clients.registry.index()

        while len(listing) > 0:
            (package, version_specifiers) = listing.popitem()

            if package not in index:
                dbt.exceptions.package_not_found(package)

            version_range = dbt.semver.reduce_versions(*version_specifiers,
                                                       name=package)

            available_versions = dbt.clients.registry.get_available_versions(
                package)

            # for now, pick a version and then recurse. later on,
            # we'll probably want to traverse multiple options
            # so we can match packages. not going to make a difference
            # right now.
            target_version = dbt.semver.resolve_to_specific_version(
                version_range,
                available_versions)

            if target_version is None:
                dbt.exceptions.package_version_not_found(
                    package, version_range, available_versions)

            version_spec = VersionSpecifier.from_version_string(target_version)
            visited_listing.incorporate(package, [version_spec])

            target_version_metadata = dbt.clients.registry.package_version(
                package, target_version)

            dependencies = target_version_metadata.get('dependencies', {})

            for dep_package, dep_versions in dependencies.items():
                versions = PackageListing.convert_version_strings(dep_versions)
                listing.incorporate(dep_package, versions)

        return visited_listing

    def fetch_required_packages(self, visited_listing):
        for package, version_specifiers in visited_listing.items():
            version_string = version_specifiers[0].to_version_string(True)
            version_info = dbt.clients.registry.package_version(
                package, version_string)

            tar_path = os.path.realpath('{}/downloads/{}.{}.tar.gz'.format(
                self.project['modules-path'],
                package,
                version_string))

            logger.info("Pulling {}@{} from hub.getdbt.com...".format(
                package, version_string))

            dbt.clients.system.make_directory(
                os.path.dirname(tar_path))

            download_url = version_info.get('downloads').get('tarball')
            dbt.clients.system.download(download_url, tar_path)

            deps_path = self.project['modules-path']
            package_name = version_info['name']
            dbt.clients.system.untar_package(tar_path, deps_path, package_name)

            logger.info(" -> Success.")
