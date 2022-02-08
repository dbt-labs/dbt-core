import os
import shutil
from typing import List

from dbt.main import handle_and_check
from dbt.logger import log_manager
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.factory import get_adapter_by_type
from dbt.events.test_types import IntegrationTestDebug
from dbt.events.functions import fire_event


# This is used in pytest tests to run dbt
def run_dbt(args: List[str] = None, expect_pass=True):
    # The logger will complain about already being initialized if
    # we don't do this.
    log_manager.reset_handlers()
    if args is None:
        args = ["run"]

    print("Invoking dbt with {}".format(args))
    res, success = handle_and_check(args)
    assert success == expect_pass, "dbt exit state did not match expected"
    return res


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


def run_sql_file(sql_path, unique_schema):
    # It would nice not to have to pass the full path in, to
    # avoid having to use the 'request' fixture.
    # Could we use os.environ['PYTEST_CURRENT_TEST']?
    # Might be more fragile, if we want to reuse this code...
    with open(sql_path, "r") as f:
        statements = f.read().split(";")
        for statement in statements:
            run_sql(statement, unique_schema)


def adapter_type():
    return "postgres"


def run_sql(sql, unique_schema, fetch=None):
    if sql.strip() == "":
        return
    # substitute schema and database in sql
    adapter = get_adapter_by_type(adapter_type())
    kwargs = {
        "schema": unique_schema,
        "database": adapter.quote("dbt"),
    }
    sql = sql.format(**kwargs)

    # get adapter and connection
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
