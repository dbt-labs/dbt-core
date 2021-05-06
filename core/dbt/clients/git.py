import re
import os.path

from dbt.clients.system import run_cmd, rmdir
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions
from packaging import version


def _is_commit(revision: str) -> bool:
    # match SHA-1 git commit
    return bool(re.match(r"\b[0-9a-f]{40}\b", revision))


def clone(repo, cwd, dirname=None, remove_git_dir=False, revision=None, subdirectory=None):
    has_revision = revision is not None
    is_commit = _is_commit(revision or "")

    clone_cmd = ['git', 'clone', '--depth', '1']
    if subdirectory:
        logger.debug('  Subdirectory specified: {}, using sparse checkout.'.format(subdirectory))
        out, _ = run_cmd(cwd, ['git', '--version'], env={'LC_ALL': 'C'})
        git_version = version.parse(re.search(r"\d+\.\d+\.\d+", out.decode("utf-8")).group(0))
        if not git_version >= version.parse("2.25.0"):
            # 2.25.0 introduces --sparse
            raise RuntimeError(
                "Please update your git version to pull a dbt package "
                "from a subdirectory: your version is {}, >= 2.25.0 needed".format(git_version)
            )
        clone_cmd.extend(['--filter=blob:none', '--sparse'])

    if has_revision and not is_commit:
        clone_cmd.extend(['--branch', revision])

    clone_cmd.append(repo)

    if dirname is not None:
        clone_cmd.append(dirname)
    result = run_cmd(cwd, clone_cmd, env={'LC_ALL': 'C'})

    if subdirectory:
        run_cmd(os.path.join(cwd, dirname or ''), ['git', 'sparse-checkout', 'set', subdirectory])

    if remove_git_dir:
        rmdir(os.path.join(dirname, '.git'))

    return result


def list_tags(cwd):
    out, err = run_cmd(cwd, ['git', 'tag', '--list'], env={'LC_ALL': 'C'})
    tags = out.decode('utf-8').strip().split("\n")
    return tags


def _checkout(cwd, repo, revision):
    logger.debug('  Checking out revision {}.'.format(revision))

    fetch_cmd = ["git", "fetch", "origin", "--depth", "1"]

    if _is_commit(revision):
        run_cmd(cwd, fetch_cmd + [revision])
    else:
        run_cmd(cwd, ['git', 'remote', 'set-branches', 'origin', revision])
        run_cmd(cwd, fetch_cmd + ["--tags", revision])

    if _is_commit(revision):
        spec = revision
    # Prefer tags to branches if one exists
    elif revision in list_tags(cwd):
        spec = 'tags/{}'.format(revision)
    else:
        spec = 'origin/{}'.format(revision)

    out, err = run_cmd(cwd, ['git', 'reset', '--hard', spec],
                       env={'LC_ALL': 'C'})
    return out, err


def checkout(cwd, repo, revision=None):
    if revision is None:
        revision = 'HEAD'
    try:
        return _checkout(cwd, repo, revision)
    except dbt.exceptions.CommandResultError as exc:
        stderr = exc.stderr.decode('utf-8').strip()
    dbt.exceptions.bad_package_spec(repo, revision, stderr)


def get_current_sha(cwd):
    out, err = run_cmd(cwd, ['git', 'rev-parse', 'HEAD'], env={'LC_ALL': 'C'})

    return out.decode('utf-8')


def remove_remote(cwd):
    return run_cmd(cwd, ['git', 'remote', 'rm', 'origin'], env={'LC_ALL': 'C'})


def clone_and_checkout(repo, cwd, dirname=None, remove_git_dir=False,
                       revision=None, subdirectory=None):
    exists = None
    try:
        _, err = clone(
            repo,
            cwd,
            dirname=dirname,
            remove_git_dir=remove_git_dir,
            subdirectory=subdirectory,
        )
    except dbt.exceptions.CommandResultError as exc:
        err = exc.stderr.decode('utf-8')
        exists = re.match("fatal: destination path '(.+)' already exists", err)
        if not exists:  # something else is wrong, raise it
            raise

    directory = None
    start_sha = None
    if exists:
        directory = exists.group(1)
        logger.debug('Updating existing dependency {}.', directory)
    else:
        matches = re.match("Cloning into '(.+)'", err.decode('utf-8'))
        if matches is None:
            raise dbt.exceptions.RuntimeException(
                f'Error cloning {repo} - never saw "Cloning into ..." from git'
            )
        directory = matches.group(1)
        logger.debug('Pulling new dependency {}.', directory)
    full_path = os.path.join(cwd, directory)
    start_sha = get_current_sha(full_path)
    checkout(full_path, repo, revision)
    end_sha = get_current_sha(full_path)
    if exists:
        if start_sha == end_sha:
            logger.debug('  Already at {}, nothing to do.', start_sha[:7])
        else:
            logger.debug('  Updated checkout from {} to {}.',
                         start_sha[:7], end_sha[:7])
    else:
        logger.debug('  Checked out at {}.', end_sha[:7])
    return os.path.join(directory, subdirectory or '')
