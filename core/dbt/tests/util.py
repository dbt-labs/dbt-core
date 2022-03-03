import os
import shutil
import yaml
import json
from typing import List

from dbt.main import handle_and_check
from dbt.logger import log_manager
from dbt.contracts.graph.manifest import Manifest
from dbt.events.functions import fire_event, capture_stdout_logs, stop_capture_stdout_logs
from dbt.events.test_types import IntegrationTestDebug
from dbt.context import providers
from unittest.mock import patch


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


def get_artifact(*paths):
    contents = read_file(*paths)
    dct = json.loads(contents)
    return dct


# For updating yaml config files
def update_config_file(updates, *paths):
    current_yaml = read_file(*paths)
    config = yaml.safe_load(current_yaml)
    config.update(updates)
    new_yaml = yaml.safe_dump(config)
    write_file(new_yaml, *paths)


def run_sql_with_adapter(adapter, sql, fetch=None):
    if sql.strip() == "":
        return
    # substitute schema and database in sql
    kwargs = {
        "schema": adapter.config.credentials.schema,
        "database": adapter.quote(adapter.config.credentials.database),
    }
    sql = sql.format(**kwargs)

    # Since the 'adapter' in dbt.adapters.factory may have been replaced by execution
    # of dbt commands since the test 'adapter' was created, we patch the 'get_adapter' call in
    # dbt.context.providers, so that macros that are called refer to this test adapter.
    # This allows tests to run normal adapter macros as if reset_adapters() were not
    # called by handle_and_check (for asserts, etc).
    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("__test"):
            conn = adapter.connections.get_thread_connection()
            msg = f'test connection "{conn.name}" executing: {sql}'
            fire_event(IntegrationTestDebug(msg=msg))
            with conn.handle.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    conn.handle.commit()
                    conn.handle.commit()
                    if fetch == "one":
                        return cursor.fetchone()
                    elif fetch == "all":
                        return cursor.fetchall()
                    else:
                        return
                except BaseException as e:
                    if conn.handle and not getattr(conn.handle, "closed", True):
                        conn.handle.rollback()
                    print(sql)
                    print(e)
                    raise
                finally:
                    conn.transaction_open = False
