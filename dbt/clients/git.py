import os.path

from dbt.clients.system import run_cmd, rmdir
from dbt.logger import GLOBAL_LOGGER as logger


def clone(repo, cwd, dirname=None, remove_git_dir=False):
    clone_cmd = ['git', 'clone', '--depth', '1', repo]

    if dirname is not None:
        clone_cmd.append(dirname)

    result = run_cmd(cwd, clone_cmd)

    if remove_git_dir:
        rmdir(os.path.join(dirname, '.git'))

    return result


def checkout(cwd, branch=None):
    if branch is None:
        branch = 'master'

    remote_branch = 'origin/{}'.format(branch)

    logger.info('  Checking out branch {}.'.format(branch))

    run_cmd(cwd, ['git', 'remote', 'set-branches', 'origin', branch])
    run_cmd(cwd, ['git', 'fetch', '--depth', '1', 'origin', branch])
    run_cmd(cwd, ['git', 'reset', '--hard', remote_branch])


def get_current_sha(cwd):
    out, err = run_cmd(cwd, ['git', 'rev-parse', 'HEAD'])

    return out.decode('utf-8')


def remove_remote(cwd):
    return run_cmd(cwd, ['git', 'remote', 'rm', 'origin'])
