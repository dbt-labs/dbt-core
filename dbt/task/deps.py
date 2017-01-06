import os
import errno
import re
import yaml
import pprint
import subprocess
import dbt.project as project

from dbt.logger import GLOBAL_LOGGER as logger


def folder_from_git_remote(remote_spec):
    start = remote_spec.rfind('/') + 1
    end = len(remote_spec) - (4 if remote_spec.endswith('.git') else 0)
    return remote_spec[start:end]


class DepsTask:
    def __init__(self, args, project):
        self.args = args
        self.project = project

    def __checkout_branch(self, branch, full_path):
        logger.info("  checking out branch {}".format(branch))
        proc = subprocess.Popen(
            ['git', 'checkout', branch],
            cwd=full_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        out, err = proc.communicate()

    def __pull_repo(self, repo, branch=None):
        proc = subprocess.Popen(
            ['git', 'clone', repo],
            cwd=self.project['modules-path'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        out, err = proc.communicate()

        exists = re.match(
            "fatal: destination path '(.+)' already exists",
            err.decode('utf-8')
        )

        folder = None
        if exists:
            folder = exists.group(1)
            logger.info("updating existing dependency {}".format(folder))
            full_path = os.path.join(self.project['modules-path'], folder)
            proc = subprocess.Popen(
                ['git', 'fetch', '--all'],
                cwd=full_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            out, err = proc.communicate()
            remote_branch = 'origin/master' if branch is None \
                            else 'origin/{}'.format(branch)
            proc = subprocess.Popen(
                ['git', 'reset', '--hard', remote_branch],
                cwd=full_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            out, err = proc.communicate()
            if branch is not None:
                self.__checkout_branch(branch, full_path)
        else:
            matches = re.match("Cloning into '(.+)'", err.decode('utf-8'))
            folder = matches.group(1)
            full_path = os.path.join(self.project['modules-path'], folder)
            logger.info("pulled new dependency {}".format(folder))
            if branch is not None:
                self.__checkout_branch(branch, full_path)

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
                    logger.info("'{}' is not a valid dbt project - "
                                "dbt_project.yml not found".format(repo))
                    exit(1)
                else:
                    raise e

    def run(self):
        if not os.path.exists(self.project['modules-path']):
            os.makedirs(self.project['modules-path'])

        self.__pull_deps_recursive(self.project['repositories'])
