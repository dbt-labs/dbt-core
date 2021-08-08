import errno
import fnmatch
import os
import os.path
import re
import shutil
import subprocess
import sys
import tarfile
import requests
from pathlib import Path
from typing import (
    Type, NoReturn, List, Optional, Dict, Any, Tuple, Union
)

import dbt.exceptions
import dbt.utils

from dbt.logger import GLOBAL_LOGGER as logger
if sys.platform == 'win32':
    from ctypes import WinDLL, c_bool
else:
    WinDLL = None
    c_bool = None


def find_matching(
    root_path: str,
    relative_paths_to_search: List[str],
    file_pattern: str,
) -> List[Dict[str, str]]:
    """
    Given an absolute `root_path`, a list of relative paths to that
    absolute root path (`relative_paths_to_search`), and a `file_pattern`
    like '*.sql', returns information about the files. For example:

    > find_matching('/root/path', ['models'], '*.sql')

      [ { 'absolute_path': '/root/path/models/model_one.sql',
          'relative_path': 'model_one.sql',
          'searched_path': 'models' },
        { 'absolute_path': '/root/path/models/subdirectory/model_two.sql',
          'relative_path': 'subdirectory/model_two.sql',
          'searched_path': 'models' } ]
    """
    matching = []
    root_path = os.path.normpath(root_path)
    regex = fnmatch.translate(file_pattern)
    reobj = re.compile(regex, re.IGNORECASE)

    for relative_path_to_search in relative_paths_to_search:
        absolute_path_to_search = os.path.join(
            root_path, relative_path_to_search)
        walk_results = os.walk(absolute_path_to_search)

        for current_path, subdirectories, local_files in walk_results:
            for local_file in local_files:
                absolute_path = os.path.join(current_path, local_file)
                relative_path = os.path.relpath(
                    absolute_path, absolute_path_to_search
                )
                if reobj.match(local_file):
                    matching.append({
                        'searched_path': relative_path_to_search,
                        'absolute_path': absolute_path,
                        'relative_path': relative_path,
                    })

    return matching


def resolve_path_from_base(path_to_resolve: str, base_path: str) -> str:
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


def open_dir_cmd() -> str:
    # https://docs.python.org/2/library/sys.html#sys.platform
    if sys.platform == 'win32':
        return 'start'

    elif sys.platform == 'darwin':
        return 'open'

    else:
        return 'xdg-open'


def _handle_posix_cwd_error(
    exc: OSError, cwd: str, cmd: List[str]
) -> NoReturn:
    if exc.errno == errno.ENOENT:
        message = 'Directory does not exist'
    elif exc.errno == errno.EACCES:
        message = 'Current user cannot access directory, check permissions'
    elif exc.errno == errno.ENOTDIR:
        message = 'Not a directory'
    else:
        message = 'Unknown OSError: {} - cwd'.format(str(exc))
    raise dbt.exceptions.WorkingDirectoryError(cwd, cmd, message)


def _handle_posix_cmd_error(
    exc: OSError, cwd: str, cmd: List[str]
) -> NoReturn:
    if exc.errno == errno.ENOENT:
        message = "Could not find command, ensure it is in the user's PATH"
    elif exc.errno == errno.EACCES:
        message = 'User does not have permissions for this command'
    else:
        message = 'Unknown OSError: {} - cmd'.format(str(exc))
    raise dbt.exceptions.ExecutableError(cwd, cmd, message)


def _handle_posix_error(exc: OSError, cwd: str, cmd: List[str]) -> NoReturn:
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


def _handle_windows_error(exc: OSError, cwd: str, cmd: List[str]) -> NoReturn:
    cls: Type[dbt.exceptions.Exception] = dbt.exceptions.CommandError
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
            str(exc), exc.errno, errno.errorcode.get(exc.errno, '<Unknown!>')
        )
    raise cls(cwd, cmd, message)


def _interpret_oserror(exc: OSError, cwd: str, cmd: List[str]) -> NoReturn:
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


def run_cmd(
    cwd: str, cmd: List[str], env: Optional[Dict[str, Any]] = None
) -> Tuple[bytes, bytes]:
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
        exe_pth = shutil.which(cmd[0])
        if exe_pth:
            cmd = [os.path.abspath(exe_pth)] + list(cmd[1:])
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=full_env)

        out, err = proc.communicate()
    except OSError as exc:
        _interpret_oserror(exc, cwd, cmd)

    logger.debug('STDOUT: "{!s}"'.format(out))
    logger.debug('STDERR: "{!s}"'.format(err))

    if proc.returncode != 0:
        logger.debug('command return code={}'.format(proc.returncode))
        raise dbt.exceptions.CommandResultError(cwd, cmd, proc.returncode,
                                                out, err)

    return out, err


def download(
    url: str, path: str, timeout: Optional[Union[float, tuple]] = None
) -> None:
    connection_timeout = timeout or float(os.getenv('DBT_HTTP_TIMEOUT', 10))
    response = requests.get(url, timeout=connection_timeout)
    with open(path, 'wb') as handle:
        for block in response.iter_content(1024 * 64):
            handle.write(block)


def untar_package(
    tar_path: str, dest_dir: str, rename_to: Optional[str] = None
) -> None:
    tar_dir_name = None
    with tarfile.open(tar_path, 'r') as tarball:
        tarball.extractall(dest_dir)
        tar_dir_name = os.path.commonprefix(tarball.getnames())
    if rename_to:
        downloaded_path = Path(os.path.join(dest_dir, tar_dir_name))
        desired_path = Path(os.path.join(dest_dir, rename_to))
        downloaded_path.rename(desired_path)
