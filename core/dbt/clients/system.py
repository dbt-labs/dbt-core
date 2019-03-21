import errno
import fnmatch
import json
import os
import os.path
import shutil
import subprocess
import sys
import tarfile
import requests
import stat

import dbt.compat
import dbt.exceptions
import dbt.utils

from dbt.logger import GLOBAL_LOGGER as logger


def find_matching(root_path,
                  relative_paths_to_search,
                  file_pattern):
    """
    Given an absolute `root_path`, a list of relative paths to that
    absolute root path (`relative_paths_to_search`), and a `file_pattern`
    like '*.sql', returns information about the files. For example:

    > find_matching('/root/path', 'models', '*.sql')

      [ { 'absolute_path': '/root/path/models/model_one.sql',
          'relative_path': 'models/model_one.sql',
          'searched_path': 'models' },
        { 'absolute_path': '/root/path/models/subdirectory/model_two.sql',
          'relative_path': 'models/subdirectory/model_two.sql',
          'searched_path': 'models' } ]
    """
    matching = []
    root_path = os.path.normpath(root_path)

    for relative_path_to_search in relative_paths_to_search:
        absolute_path_to_search = os.path.join(
            root_path, relative_path_to_search)
        walk_results = os.walk(absolute_path_to_search)

        for current_path, subdirectories, local_files in walk_results:
            for local_file in local_files:
                absolute_path = os.path.join(current_path, local_file)
                relative_path = os.path.relpath(
                    absolute_path, absolute_path_to_search)

                if fnmatch.fnmatch(local_file, file_pattern):
                    matching.append({
                        'searched_path': relative_path_to_search,
                        'absolute_path': absolute_path,
                        'relative_path': relative_path,
                    })

    return matching


def load_file_contents(path, strip=True):
    with open(path, 'rb') as handle:
        to_return = handle.read().decode('utf-8')

    if strip:
        to_return = to_return.strip()

    return to_return


def make_directory(path):
    """
    Make a directory and any intermediate directories that don't already
    exist. This function handles the case where two threads try to create
    a directory at once.
    """
    if not os.path.exists(path):
        # concurrent writes that try to create the same dir can fail
        try:
            os.makedirs(path)

        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise e


def make_file(path, contents='', overwrite=False):
    """
    Make a file at `path` assuming that the directory it resides in already
    exists. The file is saved with contents `contents`
    """
    if overwrite or not os.path.exists(path):
        with open(path, 'w') as fh:
            fh.write(contents)
        return True

    return False


def make_symlink(source, link_path):
    """
    Create a symlink at `link_path` referring to `source`.
    """
    if not supports_symlinks():
        dbt.exceptions.system_error('create a symbolic link')

    return os.symlink(source, link_path)


def supports_symlinks():
    return getattr(os, "symlink", None) is not None


def write_file(path, contents=''):
    make_directory(os.path.dirname(path))
    dbt.compat.write_file(path, contents)

    return True


def write_json(path, data):
    return write_file(path, json.dumps(data, cls=dbt.utils.JSONEncoder))


def _windows_rmdir_readonly(func, path, exc):
    exception_val = exc[1]
    if exception_val.errno == errno.EACCES:
        os.chmod(path, stat.S_IWUSR)
        func(path)
    else:
        raise


def resolve_path_from_base(path_to_resolve, base_path):
    """
    If path-to_resolve is a relative path, create an absolute path
    with base_path as the base.

    If path_to_resolve is an absolute path or a user path (~), just
    resolve it to an absolute path and return.
    """
    return os.path.abspath(
        os.path.join(
            base_path,
            os.path.expanduser(path_to_resolve)))


def rmdir(path):
    """
    Recursively deletes a directory. Includes an error handler to retry with
    different permissions on Windows. Otherwise, removing directories (eg.
    cloned via git) can cause rmtree to throw a PermissionError exception
    """
    logger.debug("DEBUG** Window rmdir sys.platform: {}".format(sys.platform))
    if sys.platform == 'win32':
        onerror = _windows_rmdir_readonly
    else:
        onerror = None

    return shutil.rmtree(path, onerror=onerror)


def remove_file(path):
    return os.remove(path)


def path_exists(path):
    return os.path.lexists(path)


def path_is_symlink(path):
    return os.path.islink(path)


def open_dir_cmd():
    # https://docs.python.org/2/library/sys.html#sys.platform
    if sys.platform == 'win32':
        return 'start'

    elif sys.platform == 'darwin':
        return 'open'

    else:
        return 'xdg-open'


def _handle_posix_cwd_error(exc, cwd, cmd):
    if exc.errno == errno.ENOENT:
        message = 'Directory does not exist'
    elif exc.errno == errno.EACCES:
        message = 'Current user cannot access directory, check permissions'
    elif exc.errno == errno.ENOTDIR:
        message = 'Not a directory'
    else:
        message = 'Unknown OSError: {} - cwd'.format(str(exc))
    raise dbt.exceptions.WorkingDirectoryError(cwd, cmd, message)


def _handle_posix_cmd_error(exc, cwd, cmd):
    if exc.errno == errno.ENOENT:
        message = "Could not find command, ensure it is in the user's PATH"
    elif exc.errno == errno.EACCES:
        message = 'User does not have permissions for this command'
    else:
        message = 'Unknown OSError: {} - cmd'.format(str(exc))
    raise dbt.exceptions.ExecutableError(cwd, cmd, message)


def _handle_posix_error(exc, cwd, cmd):
    """OSError handling for posix systems.

    Some things that could happen to trigger an OSError:
        - cwd could not exist
            - exc.errno == ENOENT
            - exc.filename == cwd
        - cwd could have permissions that prevent the current user moving to it
            - exc.errno == EACCES
            - exc.filename == cwd
        - cwd could exist but not be a directory
            - exc.errno == ENOTDIR
            - exc.filename == cwd
        - cmd[0] could not exist
            - exc.errno == ENOENT
            - exc.filename == None(?)
        - cmd[0] could exist but have permissions that prevents the current
            user from executing it (executable bit not set for the user)
            - exc.errno == EACCES
            - exc.filename == None(?)
    """
    if getattr(exc, 'filename', None) == cwd:
        _handle_posix_cwd_error(exc, cwd, cmd)
    else:
        _handle_posix_cmd_error(exc, cwd, cmd)


def _handle_windows_error(exc, cwd, cmd):
    cls = dbt.exceptions.CommandError
    if exc.errno == errno.ENOENT:
        message = ("Could not find command, ensure it is in the user's PATH "
                   "and that the user has permissions to run it")
        cls = dbt.exceptions.ExecutableError
    elif exc.errno == errno.ENOEXEC:
        message = ('Command was not executable, ensure it is valid')
        cls = dbt.exceptions.ExecutableError
    elif exc.errno == errno.ENOTDIR:
        message = ('Unable to cd: path does not exist, user does not have'
                   ' permissions, or not a directory')
        cls = dbt.exceptions.WorkingDirectoryError
    else:
        message = 'Unknown error: {} (errno={}: "{}")'.format(
            str(exc), exc.errno,  errno.errorcode.get(exc.errno, '<Unknown!>')
        )
    raise cls(cwd, cmd, message)


def _interpret_oserror(exc, cwd, cmd):
    """Interpret an OSError exc and raise the appropriate dbt exception.

    """
    if len(cmd) == 0:
        raise dbt.exceptions.CommandError(cwd, cmd)

    # all of these functions raise unconditionally
    if os.name == 'nt':
        _handle_windows_error(exc, cwd, cmd)
    else:
        _handle_posix_error(exc, cwd, cmd)

    # this should not be reachable, raise _something_ at least!
    raise dbt.exceptions.InternalException(
        'Unhandled exception in _interpret_oserror: {}'.format(exc)
    )


def run_cmd(cwd, cmd, env=None):
    logger.debug('Executing "{}"'.format(' '.join(cmd)))
    if len(cmd) == 0:
        raise dbt.exceptions.CommandError(cwd, cmd)

    # the env argument replaces the environment entirely, which has exciting
    # consequences on Windows! Do an update instead.
    full_env = env
    if env is not None:
        full_env = os.environ.copy()
        full_env.update(env)

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=full_env)

        out, err = proc.communicate()
    except OSError as exc:
        _interpret_oserror(exc, cwd, cmd)

    logger.debug('STDOUT: "{}"'.format(out))
    logger.debug('STDERR: "{}"'.format(err))

    if proc.returncode != 0:
        logger.debug('command return code={}'.format(proc.returncode))
        raise dbt.exceptions.CommandResultError(cwd, cmd, proc.returncode,
                                                out, err)

    return out, err


def download(url, path):
    response = requests.get(url)
    with open(path, 'wb') as handle:
        for block in response.iter_content(1024*64):
            handle.write(block)


def rename(from_path, to_path, force=False):
    is_symlink = path_is_symlink(to_path)

    if os.path.exists(to_path) and force:
        if is_symlink:
            remove_file(to_path)
        else:
            rmdir(to_path)

    shutil.move(from_path, to_path)


def untar_package(tar_path, dest_dir, rename_to=None):
    tar_dir_name = None
    with tarfile.open(tar_path, 'r') as tarball:
        tarball.extractall(dest_dir)
        tar_dir_name = os.path.commonprefix(tarball.getnames())
    if rename_to:
        downloaded_path = os.path.join(dest_dir, tar_dir_name)
        desired_path = os.path.join(dest_dir, rename_to)
        dbt.clients.system.rename(downloaded_path, desired_path, force=True)


def chmod_and_retry(func, path, exc_info):
    """Define an error handler to pass to shutil.rmtree.
    On Windows, when a file is marked read-only as git likes to do, rmtree will
    fail. To handle that, on errors try to make the file writable.
    We want to retry most operations here, but listdir is one that we know will
    be useless.
    """
    if func is os.listdir or os.name != 'nt':
        raise
    os.chmod(path, stat.S_IREAD | stat.S_IWRITE)
    # on error,this will raise.
    func(path)


def _absnorm(path):
    return os.path.normcase(os.path.abspath(path))


def move(src, dst):
    """A re-implementation of shutil.move that properly removes the source
    directory on windows when it has read-only files in it and the move is
    between two drives.

    This is almost identical to the real shutil.move, except it uses our rmtree
    and skips handling non-windows OSes since the existing one works ok there.
    """
    if os.name != 'nt':
        return shutil.move(src, dst)

    if os.path.isdir(dst):
        if _absnorm(src) == _absnorm(dst):
            os.rename(src, dst)
            return

        dst = os.path.join(dst, os.path.basename(src.rstrip('/\\')))
        if os.path.exists(dst):
            raise EnvironmentError("Path '{}' already exists".format(dst))

    try:
        os.rename(src, dst)
    except OSError:
        # probably different drives
        if os.path.isdir(src):
            if _absnorm(dst+'\\').startswith(_absnorm(src+'\\')):
                # dst is inside src
                raise EnvironmentError(
                    "Cannot move a directory '{}' into itself '{}'"
                    .format(src, dst)
                )
            shutil.copytree(src, dst, symlinks=True)
            rmtree(src)
        else:
            shutil.copy2(src, dst)
            os.unlink(src)


def rmtree(path):
    """Recursively remove path. On permissions errors on windows, try to remove
    the read-only flag and try again.
    """
    return shutil.rmtree(path, onerror=chmod_and_retry)
