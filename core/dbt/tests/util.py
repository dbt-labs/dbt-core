import os
import shutil
import yaml
import json
import warnings
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
    # Logbook warnings are ignored so we don't have to fork logbook to support python 3.10.
    # This _only_ works for tests in `tests/` that use the run_dbt method.
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")

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
    path = os.path.join(project_root, "target", "partial_parse.msgpack")
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

    msg = f'test connection "__test" executing: {sql}'
    fire_event(IntegrationTestDebug(msg=msg))
    # Since the 'adapter' in dbt.adapters.factory may have been replaced by execution
    # of dbt commands since the test 'adapter' was created, we patch the 'get_adapter' call in
    # dbt.context.providers, so that macros that are called refer to this test adapter.
    # This allows tests to run normal adapter macros as if reset_adapters() were not
    # called by handle_and_check (for asserts, etc).
    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("__test"):
            conn = adapter.connections.get_thread_connection()
            return adapter.run_sql_for_tests(sql, fetch, conn)


class TestProcessingException(Exception):
    pass


def relation_from_name(adapter, name: str):
    """reverse-engineer a relation from a given name and
    the adapter. The relation name is split by the '.' character.
    """

    # Different adapters have different Relation classes
    cls = adapter.Relation
    credentials = adapter.config.credentials
    quote_policy = cls.get_default_quote_policy().to_dict()
    include_policy = cls.get_default_include_policy().to_dict()

    # Make sure we have database/schema/identifier parts, even if
    # only identifier was supplied.
    relation_parts = name.split(".")
    if len(relation_parts) == 1:
        relation_parts.insert(0, credentials.schema)
    if len(relation_parts) == 2:
        relation_parts.insert(0, credentials.database)
    kwargs = {
        "database": relation_parts[0],
        "schema": relation_parts[1],
        "identifier": relation_parts[2],
    }

    relation = cls.create(
        include_policy=include_policy,
        quote_policy=quote_policy,
        **kwargs,
    )
    return relation


def check_relation_types(adapter, relation_to_type):
    """
    Relation name to table/view
    {
        "base": "table",
        "other": "view",
    }
    """

    expected_relation_values = {}
    found_relations = []
    schemas = set()

    for key, value in relation_to_type.items():
        relation = relation_from_name(adapter, key)
        expected_relation_values[relation] = value
        schemas.add(relation.without_identifier())

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("__test"):
            for schema in schemas:
                found_relations.extend(adapter.list_relations_without_caching(schema))

    for key, value in relation_to_type.items():
        for relation in found_relations:
            # this might be too broad
            if relation.identifier == key:
                assert relation.type == value, (
                    f"Got an unexpected relation type of {relation.type} "
                    f"for relation {key}, expected {value}"
                )


def check_relations_equal(adapter, relation_names):
    if len(relation_names) < 2:
        raise TestProcessingException(
            "Not enough relations to compare",
        )
    relations = [relation_from_name(adapter, name) for name in relation_names]

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("_test"):
            basis, compares = relations[0], relations[1:]
            columns = [c.name for c in adapter.get_columns_in_relation(basis)]

            for relation in compares:
                sql = adapter.get_rows_different_sql(basis, relation, column_names=columns)
                _, tbl = adapter.execute(sql, fetch=True)
                num_rows = len(tbl)
                assert (
                    num_rows == 1
                ), f"Invalid sql query from get_rows_different_sql: incorrect number of rows ({num_rows})"
                num_cols = len(tbl[0])
                assert (
                    num_cols == 2
                ), f"Invalid sql query from get_rows_different_sql: incorrect number of cols ({num_cols})"
                row_count_difference = tbl[0][0]
                assert (
                    row_count_difference == 0
                ), f"Got {row_count_difference} difference in row count betwen {basis} and {relation}"
                rows_mismatched = tbl[0][1]
                assert (
                    rows_mismatched == 0
                ), f"Got {rows_mismatched} different rows between {basis} and {relation}"


def get_unique_ids_in_results(results):
    unique_ids = []
    for result in results:
        unique_ids.append(result.node.unique_id)
    return unique_ids


def check_result_nodes_by_name(results, names):
    result_names = []
    for result in results:
        result_names.append(result.node.name)
    assert set(names) == set(result_names)


def check_result_nodes_by_unique_id(results, unique_ids):
    result_unique_ids = []
    for result in results:
        result_unique_ids.append(result.node.unique_id)
    assert set(unique_ids) == set(result_unique_ids)


def update_rows(adapter, update_rows_config):
    """
    {
      "name": "base",
      "dst_col": "some_date"
      "clause": {
          "type": "add_timestamp",
          "src_col": "some_date",
       "where" "id > 10"
    }
    """
    for key in ["name", "dst_col", "clause"]:
        if key not in update_rows_config:
            raise TestProcessingException(f"Invalid update_rows: no {key}")

    clause = update_rows_config["clause"]
    clause = generate_update_clause(adapter, clause)

    where = None
    if "where" in update_rows_config:
        where = update_rows_config["where"]

    name = update_rows_config["name"]
    dst_col = update_rows_config["dst_col"]
    relation = relation_from_name(adapter, name)

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("_test"):
            sql = adapter.update_column_sql(
                dst_name=str(relation),
                dst_column=dst_col,
                clause=clause,
                where_clause=where,
            )
            adapter.execute(sql, auto_begin=True)
            adapter.commit_if_has_connection()


def generate_update_clause(adapter, clause) -> str:
    """
    Called by update_rows function. Expects the "clause" dictionary
    documented in 'update_rows.
    """

    if "type" not in clause or clause["type"] not in ["add_timestamp", "add_string"]:
        raise TestProcessingException("invalid update_rows clause: type missing or incorrect")
    clause_type = clause["type"]

    if clause_type == "add_timestamp":
        if "src_col" not in clause:
            raise TestProcessingException("Invalid update_rows clause: no src_col")
        add_to = clause["src_col"]
        kwargs = {k: v for k, v in clause.items() if k in ("interval", "number")}
        with patch.object(providers, "get_adapter", return_value=adapter):
            with adapter.connection_named("_test"):
                return adapter.timestamp_add_sql(add_to=add_to, **kwargs)
    elif clause_type == "add_string":
        for key in ["src_col", "value"]:
            if key not in clause:
                raise TestProcessingException(f"Invalid update_rows clause: no {key}")
        src_col = clause["src_col"]
        value = clause["value"]
        location = clause.get("location", "append")
        with patch.object(providers, "get_adapter", return_value=adapter):
            with adapter.connection_named("_test"):
                return adapter.string_add_sql(src_col, value, location)
    return ""
