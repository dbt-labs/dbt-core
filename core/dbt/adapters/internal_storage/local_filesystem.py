from pathlib import Path
from shutil import rmtree
from stat import S_IRUSR, S_IWUSR
from sys import platform
from typing import Any, Union
from typing_extensions import Literal

from dbt.logger import GLOBAL_LOGGER as logger

# windows platforms as returned by sys.platform
# via https://stackoverflow.com/a/13874620
WINDOWS_PLATFORMS = ("win32", "cygwin", "msys")

# load ctypes for windows platforms
if platform in WINDOWS_PLATFORMS:
    from ctypes import WinDLL, c_bool
else:
    WinDLL = None
    c_bool = None


def ready_check() -> bool:
    """Ensures the adapter is ready for use.

    Returns:
        `True` if the resource is ready to be used.
        `False` if the resource is not ready to be used.

    Raises:
        TBD - TODO: How best to report back errors here?
        It should never fail for a filesystem (unless we're using something very exotic),
        but for databases this should be our primary source of troubleshooting information.

    """
    return True


def read(
    path: str,
    strip: bool = True,
) -> str:
    """Reads the content of a file on the filesystem.

    Args:
        path: Full path of file to be read.
        strip: Wether or not to strip whitespace.

    Returns:
        Content of the file

    """
    # create a concrete path object
    path: Path = Path(path)

    # read the file in as a string, or none if not found
    file_content = path.read_text(encoding="utf-8")

    # conditionally strip whitespace
    file_content = file_content.strip() if strip else file_content

    return file_content


def write(
    path: str,
    content: Union[str, bytes, None],
    overwrite: bool = True,
) -> bool:
    """Writes the given content out to a resource on the filesystem.

    Since there are many different ways to approach filesystem operations, It's best to enumerate
     the rules(tm):

    If the path to the file/dir doesn't exist, it will be created.
    If content is `None` a directory will be created instead of a file.
    If contet is not a `str` or `bytes` the string representation of the object will be written.
    If the content is `str` it will be encoded as utf-8

    Overwrites are only supported for files and are toggled by the overwrite parameter.

    All logical cases outside those outlined above will result in failure

    Args:
        path: Full path of resource to be written.
        content: Data to be written.
        overwrite: Wether or not to overwrite if a file already exists at this path.
        parser: A parser to apply to file data.

    Returns:
        `True` for success, `False` otherwise.

    """
    # create a concrete path object
    path: Path = Path(path)

    # handle overwrite errors for files and directories
    if path.exists() and (overwrite is False or path.is_dir() is True):
        logger.debug(f"{path} already exists")
        return False

    # handle trying to write file content to a path that is a directory
    if path.is_dir() and content is not None:
        logger.debug(f"{path} is a directory, but file content was specified")
        return False

    # create a directory if the content is `None`
    if content is None:
        path.mkdir(parents=True, exist_ok=True)

    # create an empty file if the content is an empty string
    elif content == "" and path.exists() is False:
        path.touch()

    # write out to a file
    else:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if type(content) == bytes:
                path.write_bytes(content)
            else:
                path.write_text(str(content), encoding="utf-8")
        except Exception as e:
            # TODO: The block below was c/p'd directly from the old system client.
            #   We should examine if this actually makes sense to log file write failures and
            #   try to keep going.
            if platform in WINDOWS_PLATFORMS:
                # sometimes we get a winerror of 3 which means the path was
                # definitely too long, but other times we don't and it means the
                # path was just probably too long. This is probably based on the
                # windows/python version.
                if getattr(e, "winerror", 0) == 3:
                    reason = "Path was too long"
                else:
                    reason = "Path was possibly too long"
                # all our hard work and the path was still too long. Log and
                # continue.
                logger.debug(
                    f"Could not write to path {path}({len(path)} characters): "
                    f"{reason}\nexception: {e}"
                )
            else:
                raise

    return True


def delete(path: str) -> bool:
    """Deletes the resource at the given path

    Args:
        path: Full path of resource to be deleted

    Returns:
        `True` for success, `False` otherwise.

    """
    # create concrete path object
    path: Path = Path(path)

    # ensure resource to be deleted exists
    if not path.exists():
        return False

    # remove files
    if path.is_file():
        path.unlink()

    # remove directories recursively, surprisingly obnoxious to do in a cross-platform safe manner
    if path.is_dir():
        if platform in WINDOWS_PLATFORMS:
            # error handling for permissions on windows platforms
            def on_error(func, path, _):
                path.chmod(path, S_IWUSR | S_IRUSR)
                func(path)

            rmtree(path, onerror=on_error)
        else:
            rmtree(path)

    return True


def info(path: str) -> Union[dict, Literal[False]]:
    """Provides information about what is found at the given path.

    If info is called on a directory the size will be `None`
    if Info is called on a resource that does not exist the response will be `False`

    N.B: despite my best efforts, getting a reliable cross-platform file creation time is
    absurdly difficult.
    See these links for information if we ever decide we have to have this feature:
      * https://bugs.python.org/issue39533
      * https://github.com/ipfs-shipyard/py-datastore/blob/e566d40a8ca81d8628147e255fe7830b5f928a43/datastore/filesystem/util/statx.py # noqa: E501
      * https://github.com/ckarageorgkaneen/pystatx

    Args:
        path: Full path being queried.

    Returns:
        On success: A dict containing information about what is found at the given path.
        On failure: `False`

    """
    # create a concrete path object.
    path: Path = Path(path)

    # return `False` if the resource doesn't exsist
    if not path.exists():
        return False

    # calulate file size (`None` for dirs)
    size = None if path.is_dir() else path.stat().st_size

    # return info on the resource
    return {"size": size, "modified_at": path.stat().st_mtime}
