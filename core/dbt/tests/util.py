import os
import shutil
from typing import List

from dbt.main import handle_and_check
from dbt.logger import log_manager
from dbt.contracts.graph.manifest import Manifest
from dbt.events.functions import capture_stdout_logs, stop_capture_stdout_logs


# This is used in pytest tests to run dbt
def run_dbt(args: List[str] = None, expect_pass=True):
    # The logger will complain about already being initialized if
    # we don't do this.
    log_manager.reset_handlers()
    if args is None:
        args = ["run"]

    print("\n\nInvoking dbt with {}".format(args))
    res, success = handle_and_check(args)
    #   assert success == expect_pass, "dbt exit state did not match expected"
    return res


def run_dbt_and_capture(args: List[str] = None, expect_pass=True):
    try:
        stringbuf = capture_stdout_logs()
        res = run_dbt(args, expect_pass=expect_pass)
        stdout = stringbuf.getvalue()

    finally:
        stop_capture_stdout_logs()

    return res, stdout


# Used in test cases to get the manifest from the partial parsing file
def get_manifest(project_root):
    path = project_root.join("target", "partial_parse.msgpack")
    if os.path.exists(path):
        with open(path, "rb") as fp:
            manifest_mp = fp.read()
        manifest: Manifest = Manifest.from_msgpack(manifest_mp)
        return manifest
    else:
        return None


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


def copy_file(src_path, src, dest_path, dest) -> None:
    # dest is a list, so that we can provide nested directories, like 'models' etc.
    # copy files from the data_dir to appropriate project directory
    shutil.copyfile(
        os.path.join(src_path, src),
        os.path.join(dest_path, *dest),
    )


def rm_file(src_path, src) -> None:
    # remove files from proj_path
    os.remove(os.path.join(src_path, src))


# We need to explicitly use encoding="utf-8" because otherwise on
# Windows we'll get codepage 1252 and things might break
def write_file(contents, *paths):
    with open(os.path.join(*paths), "w", encoding="utf-8") as fp:
        fp.write(contents)


def read_file(*paths):
    contents = ""
    with open(os.path.join(*paths), "r") as fp:
        contents = fp.read()
    return contents
