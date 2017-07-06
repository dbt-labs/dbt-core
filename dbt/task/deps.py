import os
import errno
import re

import dbt.clients.git
import dbt.clients.system
import dbt.project as project

from dbt.compat import basestring
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.task.base_task import BaseTask


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
        dbt.clients.system.make_directory(self.project['modules-path'])

        self.__pull_deps_recursive(self.project['repositories'])
