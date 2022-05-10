import hashlib
import dbt
import os
from unittest.mock import ANY
from dbt.tests.util import AnyStringWith

# This produces an "expected manifest", with a number of the fields
# modified to avoid ephemeral changes.
#   ANY
#   AnyStringWith
#   LineIndifferent
# It also uses some convenience methods to generate the
# various config dictionaries.


def get_rendered_model_config(**updates):
    result = {
        "database": None,
        "schema": None,
        "alias": None,
        "enabled": True,
        "materialized": "view",
        "pre-hook": [],
        "post-hook": [],
        "column_types": {},
        "quoting": {},
        "tags": [],
        "persist_docs": {},
        "full_refresh": None,
        "on_schema_change": "ignore",
        "meta": {},
        "unique_key": None,
        "grants": {},
    }
    result.update(updates)
    return result


def get_unrendered_model_config(**updates):
    return updates


def get_rendered_seed_config(**updates):
    result = {
        "enabled": True,
        "materialized": "seed",
        "persist_docs": {},
        "pre-hook": [],
        "post-hook": [],
        "column_types": {},
        "quoting": {},
        "tags": [],
        "quote_columns": True,
        "full_refresh": None,
        "on_schema_change": "ignore",
        "database": None,
        "schema": None,
        "alias": None,
        "meta": {},
        "unique_key": None,
        "grants": {},
    }
    result.update(updates)
    return result


def get_unrendered_seed_config(**updates):
    result = {"quote_columns": True}
    result.update(updates)
    return result


def get_rendered_snapshot_config(**updates):
    result = {
        "database": None,
        "schema": None,
        "alias": None,
        "enabled": True,
        "materialized": "snapshot",
        "pre-hook": [],
        "post-hook": [],
        "column_types": {},
        "quoting": {},
        "tags": [],
        "persist_docs": {},
        "full_refresh": None,
        "on_schema_change": "ignore",
        "strategy": "check",
        "check_cols": "all",
        "unique_key": "id",
        "target_schema": None,
        "meta": {},
        "grants": {},
    }
    result.update(updates)
    return result


def get_unrendered_snapshot_config(**updates):
    result = {"check_cols": "all", "strategy": "check", "target_schema": None, "unique_key": "id"}
    result.update(updates)
    return result


def get_rendered_tst_config(**updates):
    result = {
        "enabled": True,
        "materialized": "test",
        "tags": [],
        "severity": "ERROR",
        "store_failures": None,
        "warn_if": "!= 0",
        "error_if": "!= 0",
        "fail_calc": "count(*)",
        "where": None,
        "limit": None,
        "database": None,
        "schema": "dbt_test__audit",
        "alias": None,
        "meta": {},
    }
    result.update(updates)
    return result


def get_unrendered_tst_config(**updates):
    result = {}
    result.update(updates)
    return result


def quote(value):
    quote_char = '"'
    return "{0}{1}{0}".format(quote_char, value)


def relation_name_format(quote_database: bool, quote_schema: bool, quote_identifier: bool):
    return ".".join(
        (
            quote("{0}") if quote_database else "{0}",
            quote("{1}") if quote_schema else "{1}",
            quote("{2}") if quote_identifier else "{2}",
        )
    )


def checksum_file(path):
    """windows has silly git behavior that adds newlines, and python does
    silly things if we just open(..., 'r').encode('utf-8').
    """
    with open(path, "rb") as fp:
        hashed = hashlib.sha256(fp.read()).hexdigest()
    return {
        "name": "sha256",
        "checksum": hashed,
    }


def read_file_replace_returns(path):
    with open(path, "r") as fp:
        return fp.read().replace("\r", "").replace("\n", "")


class LineIndifferent:
    def __init__(self, expected):
        self.expected = expected.replace("\r", "")

    def __eq__(self, other):
        got = other.replace("\r", "").replace("\n", "")
        return self.expected == got

    def __repr__(self):
        return "LineIndifferent({!r})".format(self.expected)

    def __str__(self):
        return self.__repr__()


def expected_seeded_manifest(project, model_database=None, quote_model=False):

    model_sql_path = os.path.join("models", "model.sql")
    second_model_sql_path = os.path.join("models", "second_model.sql")
    model_schema_yml_path = os.path.join("models", "schema.yml")
    seed_schema_yml_path = os.path.join("seeds", "schema.yml")
    seed_path = os.path.join("seeds", "seed.csv")
    snapshot_path = os.path.join("snapshots", "snapshot_seed.sql")

    my_schema_name = project.test_schema
    alternate_schema = project.test_schema + "_test"
    test_audit_schema = my_schema_name + "_dbt_test__audit"

    model_database = project.database

    model_config = get_rendered_model_config()
    second_config = get_rendered_model_config(schema="test")

    unrendered_model_config = get_unrendered_model_config(materialized="view")

    unrendered_second_config = get_unrendered_model_config(schema="test", materialized="view")

    seed_config = get_rendered_seed_config()
    unrendered_seed_config = get_unrendered_seed_config()

    test_config = get_rendered_tst_config()
    unrendered_test_config = get_unrendered_tst_config()

    snapshot_config = get_rendered_snapshot_config(target_schema=alternate_schema)
    unrendered_snapshot_config = get_unrendered_snapshot_config(target_schema=alternate_schema)

    quote_database = quote_schema = True
    relation_name_node_format = relation_name_format(quote_database, quote_schema, quote_model)
    relation_name_source_format = relation_name_format(
        quote_database, quote_schema, quote_identifier=True
    )

    compiled_model_path = os.path.join("target", "compiled", "test", "models")

    model_raw_sql = read_file_replace_returns(model_sql_path).rstrip("\r\n")

    return {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v5.json",
        "dbt_version": dbt.version.__version__,
        "nodes": {
            "model.test.model": {
                "compiled_path": os.path.join(compiled_model_path, "model.sql"),
                "build_path": None,
                "created_at": ANY,
                "name": "model",
                "root_path": project.project_root,
                "relation_name": relation_name_node_format.format(
                    model_database, my_schema_name, "model"
                ),
                "resource_type": "model",
                "path": "model.sql",
                "original_file_path": model_sql_path,
                "package_name": "test",
                "raw_sql": LineIndifferent(model_raw_sql),
                "refs": [["seed"]],
                "sources": [],
                "depends_on": {"nodes": ["seed.test.seed"], "macros": []},
                "unique_id": "model.test.model",
                "fqn": ["test", "model"],
                "tags": [],
                "meta": {},
                "config": model_config,
                "schema": my_schema_name,
                "database": model_database,
                "deferred": False,
                "alias": "model",
                "description": "The test model",
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "The user ID number",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "first_name": {
                        "name": "first_name",
                        "description": "The user's first name",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "email": {
                        "name": "email",
                        "description": "The user's email",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "ip_address": {
                        "name": "ip_address",
                        "description": "The user's IP address",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "updated_at": {
                        "name": "updated_at",
                        "description": "The last time this user's email was updated",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                },
                "patch_path": "test://" + model_schema_yml_path,
                "docs": {"show": False},
                "compiled": True,
                "compiled_sql": ANY,
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "checksum": checksum_file(model_sql_path),
                "unrendered_config": unrendered_model_config,
            },
            "model.test.second_model": {
                "compiled_path": os.path.join(compiled_model_path, "second_model.sql"),
                "build_path": None,
                "created_at": ANY,
                "name": "second_model",
                "root_path": project.project_root,
                "relation_name": relation_name_node_format.format(
                    project.database, alternate_schema, "second_model"
                ),
                "resource_type": "model",
                "path": "second_model.sql",
                "original_file_path": second_model_sql_path,
                "package_name": "test",
                "raw_sql": LineIndifferent(
                    read_file_replace_returns(second_model_sql_path).rstrip("\r\n")
                ),
                "refs": [["seed"]],
                "sources": [],
                "depends_on": {"nodes": ["seed.test.seed"], "macros": []},
                "unique_id": "model.test.second_model",
                "fqn": ["test", "second_model"],
                "tags": [],
                "meta": {},
                "config": second_config,
                "schema": alternate_schema,
                "database": project.database,
                "deferred": False,
                "alias": "second_model",
                "description": "The second test model",
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "The user ID number",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "first_name": {
                        "name": "first_name",
                        "description": "The user's first name",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "email": {
                        "name": "email",
                        "description": "The user's email",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "ip_address": {
                        "name": "ip_address",
                        "description": "The user's IP address",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "updated_at": {
                        "name": "updated_at",
                        "description": "The last time this user's email was updated",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                },
                "patch_path": "test://" + model_schema_yml_path,
                "docs": {"show": False},
                "compiled": True,
                "compiled_sql": ANY,
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "checksum": checksum_file(second_model_sql_path),
                "unrendered_config": unrendered_second_config,
            },
            "seed.test.seed": {
                "compiled_path": None,
                "build_path": None,
                "created_at": ANY,
                "compiled": True,
                "compiled_sql": "",
                "config": seed_config,
                "patch_path": "test://" + seed_schema_yml_path,
                "path": "seed.csv",
                "name": "seed",
                "root_path": project.project_root,
                "relation_name": relation_name_node_format.format(
                    project.database, my_schema_name, "seed"
                ),
                "resource_type": "seed",
                "raw_sql": "",
                "package_name": "test",
                "original_file_path": seed_path,
                "refs": [],
                "sources": [],
                "depends_on": {"nodes": [], "macros": []},
                "unique_id": "seed.test.seed",
                "fqn": ["test", "seed"],
                "tags": [],
                "meta": {},
                "schema": my_schema_name,
                "database": project.database,
                "alias": "seed",
                "deferred": False,
                "description": "The test seed",
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "The user ID number",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "first_name": {
                        "name": "first_name",
                        "description": "The user's first name",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "email": {
                        "name": "email",
                        "description": "The user's email",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "ip_address": {
                        "name": "ip_address",
                        "description": "The user's IP address",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "updated_at": {
                        "name": "updated_at",
                        "description": "The last time this user's email was updated",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                },
                "docs": {"show": True},
                "compiled": True,
                "compiled_sql": "",
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "checksum": checksum_file(seed_path),
                "unrendered_config": unrendered_seed_config,
            },
            "test.test.not_null_model_id.d01cc630e6": {
                "alias": "not_null_model_id",
                "compiled_path": os.path.join(
                    compiled_model_path, "schema.yml", "not_null_model_id.sql"
                ),
                "build_path": None,
                "created_at": ANY,
                "column_name": "id",
                "columns": {},
                "config": test_config,
                "sources": [],
                "depends_on": {
                    "macros": ["macro.dbt.test_not_null", "macro.dbt.get_where_subquery"],
                    "nodes": ["model.test.model"],
                },
                "deferred": False,
                "description": "",
                "file_key_name": "models.model",
                "fqn": ["test", "not_null_model_id"],
                "name": "not_null_model_id",
                "original_file_path": model_schema_yml_path,
                "package_name": "test",
                "patch_path": None,
                "path": "not_null_model_id.sql",
                "raw_sql": "{{ test_not_null(**_dbt_generic_test_kwargs) }}",
                "refs": [["model"]],
                "relation_name": None,
                "resource_type": "test",
                "root_path": project.project_root,
                "schema": test_audit_schema,
                "database": project.database,
                "tags": [],
                "meta": {},
                "unique_id": "test.test.not_null_model_id.d01cc630e6",
                "docs": {"show": True},
                "compiled": True,
                "compiled_sql": AnyStringWith("where id is null"),
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "test_metadata": {
                    "namespace": None,
                    "name": "not_null",
                    "kwargs": {
                        "column_name": "id",
                        "model": "{{ get_where_subquery(ref('model')) }}",
                    },
                },
                "checksum": {"name": "none", "checksum": ""},
                "unrendered_config": unrendered_test_config,
            },
            "snapshot.test.snapshot_seed": {
                "alias": "snapshot_seed",
                "compiled_path": None,
                "build_path": None,
                "created_at": ANY,
                "checksum": checksum_file(snapshot_path),
                "columns": {},
                "compiled": True,
                "compiled_sql": ANY,
                "config": snapshot_config,
                "database": project.database,
                "deferred": False,
                "depends_on": {
                    "macros": [],
                    "nodes": ["seed.test.seed"],
                },
                "description": "",
                "docs": {"show": True},
                "extra_ctes": [],
                "extra_ctes_injected": True,
                "fqn": ["test", "snapshot_seed", "snapshot_seed"],
                "meta": {},
                "name": "snapshot_seed",
                "original_file_path": snapshot_path,
                "package_name": "test",
                "patch_path": None,
                "path": "snapshot_seed.sql",
                "raw_sql": LineIndifferent(
                    read_file_replace_returns(snapshot_path)
                    .replace("{% snapshot snapshot_seed %}", "")
                    .replace("{% endsnapshot %}", "")
                ),
                "refs": [["seed"]],
                "relation_name": relation_name_node_format.format(
                    project.database, alternate_schema, "snapshot_seed"
                ),
                "resource_type": "snapshot",
                "root_path": project.project_root,
                "schema": alternate_schema,
                "sources": [],
                "tags": [],
                "unique_id": "snapshot.test.snapshot_seed",
                "unrendered_config": unrendered_snapshot_config,
            },
            "test.test.test_nothing_model_.5d38568946": {
                "alias": "test_nothing_model_",
                "compiled_path": os.path.join(
                    compiled_model_path, "schema.yml", "test_nothing_model_.sql"
                ),
                "build_path": None,
                "created_at": ANY,
                "column_name": None,
                "columns": {},
                "config": test_config,
                "sources": [],
                "depends_on": {
                    "macros": ["macro.test.test_nothing", "macro.dbt.get_where_subquery"],
                    "nodes": ["model.test.model"],
                },
                "deferred": False,
                "description": "",
                "file_key_name": "models.model",
                "fqn": ["test", "test_nothing_model_"],
                "name": "test_nothing_model_",
                "original_file_path": model_schema_yml_path,
                "package_name": "test",
                "patch_path": None,
                "path": "test_nothing_model_.sql",
                "raw_sql": "{{ test.test_nothing(**_dbt_generic_test_kwargs) }}",
                "refs": [["model"]],
                "relation_name": None,
                "resource_type": "test",
                "root_path": project.project_root,
                "schema": test_audit_schema,
                "database": project.database,
                "tags": [],
                "meta": {},
                "unique_id": "test.test.test_nothing_model_.5d38568946",
                "docs": {"show": True},
                "compiled": True,
                "compiled_sql": AnyStringWith("select 0"),
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "test_metadata": {
                    "namespace": "test",
                    "name": "nothing",
                    "kwargs": {
                        "model": "{{ get_where_subquery(ref('model')) }}",
                    },
                },
                "checksum": {"name": "none", "checksum": ""},
                "unrendered_config": unrendered_test_config,
            },
            "test.test.unique_model_id.67b76558ff": {
                "alias": "unique_model_id",
                "compiled_path": os.path.join(
                    compiled_model_path, "schema.yml", "unique_model_id.sql"
                ),
                "build_path": None,
                "created_at": ANY,
                "column_name": "id",
                "columns": {},
                "config": test_config,
                "sources": [],
                "depends_on": {
                    "macros": ["macro.dbt.test_unique", "macro.dbt.get_where_subquery"],
                    "nodes": ["model.test.model"],
                },
                "deferred": False,
                "description": "",
                "file_key_name": "models.model",
                "fqn": ["test", "unique_model_id"],
                "name": "unique_model_id",
                "original_file_path": model_schema_yml_path,
                "package_name": "test",
                "patch_path": None,
                "path": "unique_model_id.sql",
                "raw_sql": "{{ test_unique(**_dbt_generic_test_kwargs) }}",
                "refs": [["model"]],
                "relation_name": None,
                "resource_type": "test",
                "root_path": project.project_root,
                "schema": test_audit_schema,
                "database": project.database,
                "tags": [],
                "meta": {},
                "unique_id": "test.test.unique_model_id.67b76558ff",
                "docs": {"show": True},
                "compiled": True,
                "compiled_sql": AnyStringWith("count(*)"),
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "test_metadata": {
                    "namespace": None,
                    "name": "unique",
                    "kwargs": {
                        "column_name": "id",
                        "model": "{{ get_where_subquery(ref('model')) }}",
                    },
                },
                "checksum": {"name": "none", "checksum": ""},
                "unrendered_config": unrendered_test_config,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "created_at": ANY,
                "columns": {
                    "id": {
                        "description": "An ID field",
                        "name": "id",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    }
                },
                "config": {
                    "enabled": True,
                },
                "quoting": {
                    "database": None,
                    "schema": None,
                    "identifier": True,
                    "column": None,
                },
                "database": project.database,
                "description": "My table",
                "external": None,
                "freshness": {
                    "error_after": {"count": None, "period": None},
                    "warn_after": {"count": None, "period": None},
                    "filter": None,
                },
                "identifier": "seed",
                "loaded_at_field": None,
                "loader": "a_loader",
                "meta": {},
                "name": "my_table",
                "original_file_path": os.path.join("models", "schema.yml"),
                "package_name": "test",
                "path": os.path.join("models", "schema.yml"),
                "patch_path": None,
                "relation_name": relation_name_source_format.format(
                    project.database, my_schema_name, "seed"
                ),
                "resource_type": "source",
                "root_path": project.project_root,
                "schema": my_schema_name,
                "source_description": "My source",
                "source_name": "my_source",
                "source_meta": {},
                "tags": [],
                "unique_id": "source.test.my_source.my_table",
                "fqn": ["test", "my_source", "my_table"],
                "unrendered_config": {},
            },
        },
        "exposures": {
            "exposure.test.notebook_exposure": {
                "created_at": ANY,
                "depends_on": {
                    "macros": [],
                    "nodes": ["model.test.model", "model.test.second_model"],
                },
                "description": "A description of the complex exposure\n",
                "fqn": ["test", "notebook_exposure"],
                "maturity": "medium",
                "meta": {"tool": "my_tool", "languages": ["python"]},
                "tags": ["my_department"],
                "name": "notebook_exposure",
                "original_file_path": os.path.join("models", "schema.yml"),
                "owner": {"email": "something@example.com", "name": "Some name"},
                "package_name": "test",
                "path": "schema.yml",
                "refs": [["model"], ["second_model"]],
                "resource_type": "exposure",
                "root_path": project.project_root,
                "sources": [],
                "type": "notebook",
                "unique_id": "exposure.test.notebook_exposure",
                "url": "http://example.com/notebook/1",
            },
            "exposure.test.simple_exposure": {
                "created_at": ANY,
                "depends_on": {
                    "macros": [],
                    "nodes": ["source.test.my_source.my_table", "model.test.model"],
                },
                "description": "",
                "fqn": ["test", "simple_exposure"],
                "name": "simple_exposure",
                "original_file_path": os.path.join("models", "schema.yml"),
                "owner": {
                    "email": "something@example.com",
                    "name": None,
                },
                "package_name": "test",
                "path": "schema.yml",
                "refs": [["model"]],
                "resource_type": "exposure",
                "root_path": project.project_root,
                "sources": [["my_source", "my_table"]],
                "type": "dashboard",
                "unique_id": "exposure.test.simple_exposure",
                "url": None,
                "maturity": None,
                "meta": {},
                "tags": [],
            },
        },
        "metrics": {},
        "selectors": {},
        "parent_map": {
            "model.test.model": ["seed.test.seed"],
            "model.test.second_model": ["seed.test.seed"],
            "exposure.test.notebook_exposure": ["model.test.model", "model.test.second_model"],
            "exposure.test.simple_exposure": [
                "model.test.model",
                "source.test.my_source.my_table",
            ],
            "seed.test.seed": [],
            "snapshot.test.snapshot_seed": ["seed.test.seed"],
            "source.test.my_source.my_table": [],
            "test.test.not_null_model_id.d01cc630e6": ["model.test.model"],
            "test.test.test_nothing_model_.5d38568946": ["model.test.model"],
            "test.test.unique_model_id.67b76558ff": ["model.test.model"],
        },
        "child_map": {
            "model.test.model": [
                "exposure.test.notebook_exposure",
                "exposure.test.simple_exposure",
                "test.test.not_null_model_id.d01cc630e6",
                "test.test.test_nothing_model_.5d38568946",
                "test.test.unique_model_id.67b76558ff",
            ],
            "model.test.second_model": ["exposure.test.notebook_exposure"],
            "exposure.test.notebook_exposure": [],
            "exposure.test.simple_exposure": [],
            "seed.test.seed": [
                "model.test.model",
                "model.test.second_model",
                "snapshot.test.snapshot_seed",
            ],
            "snapshot.test.snapshot_seed": [],
            "source.test.my_source.my_table": ["exposure.test.simple_exposure"],
            "test.test.not_null_model_id.d01cc630e6": [],
            "test.test.test_nothing_model_.5d38568946": [],
            "test.test.unique_model_id.67b76558ff": [],
        },
        "docs": {
            "dbt.__overview__": ANY,
            "test.macro_info": ANY,
            "test.macro_arg_info": ANY,
        },
        "disabled": {},
    }


def expected_references_manifest(project):
    model_database = project.database
    my_schema_name = project.test_schema
    docs_path = os.path.join("models", "docs.md")
    ephemeral_copy_path = os.path.join("models", "ephemeral_copy.sql")
    ephemeral_summary_path = os.path.join("models", "ephemeral_summary.sql")
    view_summary_path = os.path.join("models", "view_summary.sql")
    seed_path = os.path.join("seeds", "seed.csv")
    snapshot_path = os.path.join("snapshots", "snapshot_seed.sql")
    compiled_model_path = os.path.join("target", "compiled", "test", "models")
    schema_yml_path = os.path.join("models", "schema.yml")

    ephemeral_copy_sql = read_file_replace_returns(ephemeral_copy_path).rstrip("\r\n")
    ephemeral_summary_sql = read_file_replace_returns(ephemeral_summary_path).rstrip("\r\n")
    view_summary_sql = read_file_replace_returns(view_summary_path).rstrip("\r\n")
    alternate_schema = project.test_schema + "_test"

    return {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v5.json",
        "dbt_version": dbt.version.__version__,
        "nodes": {
            "model.test.ephemeral_copy": {
                "alias": "ephemeral_copy",
                "compiled_path": os.path.join(compiled_model_path, "ephemeral_copy.sql"),
                "build_path": None,
                "created_at": ANY,
                "columns": {},
                "config": get_rendered_model_config(materialized="ephemeral"),
                "sources": [["my_source", "my_table"]],
                "depends_on": {"macros": [], "nodes": ["source.test.my_source.my_table"]},
                "deferred": False,
                "description": "",
                "docs": {"show": True},
                "fqn": ["test", "ephemeral_copy"],
                "name": "ephemeral_copy",
                "original_file_path": ephemeral_copy_path,
                "package_name": "test",
                "patch_path": None,
                "path": "ephemeral_copy.sql",
                "raw_sql": LineIndifferent(ephemeral_copy_sql),
                "refs": [],
                "relation_name": None,
                "resource_type": "model",
                "root_path": project.project_root,
                "schema": my_schema_name,
                "database": project.database,
                "tags": [],
                "meta": {},
                "unique_id": "model.test.ephemeral_copy",
                "compiled": True,
                "compiled_sql": ANY,
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "checksum": checksum_file(ephemeral_copy_path),
                "unrendered_config": get_unrendered_model_config(materialized="ephemeral"),
            },
            "model.test.ephemeral_summary": {
                "alias": "ephemeral_summary",
                "compiled_path": os.path.join(compiled_model_path, "ephemeral_summary.sql"),
                "build_path": None,
                "created_at": ANY,
                "columns": {
                    "first_name": {
                        "description": "The first name being summarized",
                        "name": "first_name",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "ct": {
                        "description": "The number of instances of the first name",
                        "name": "ct",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                },
                "config": get_rendered_model_config(materialized="table"),
                "sources": [],
                "depends_on": {"macros": [], "nodes": ["model.test.ephemeral_copy"]},
                "deferred": False,
                "description": "A summmary table of the ephemeral copy of the seed data",
                "docs": {"show": True},
                "fqn": ["test", "ephemeral_summary"],
                "name": "ephemeral_summary",
                "original_file_path": ephemeral_summary_path,
                "package_name": "test",
                "patch_path": "test://" + os.path.join("models", "schema.yml"),
                "path": "ephemeral_summary.sql",
                "raw_sql": LineIndifferent(ephemeral_summary_sql),
                "refs": [["ephemeral_copy"]],
                "relation_name": '"{0}"."{1}".ephemeral_summary'.format(
                    model_database, my_schema_name
                ),
                "resource_type": "model",
                "root_path": project.project_root,
                "schema": my_schema_name,
                "database": project.database,
                "tags": [],
                "meta": {},
                "unique_id": "model.test.ephemeral_summary",
                "compiled": True,
                "compiled_sql": ANY,
                "extra_ctes_injected": True,
                "extra_ctes": [ANY],
                "checksum": checksum_file(ephemeral_summary_path),
                "unrendered_config": get_unrendered_model_config(materialized="table"),
            },
            "model.test.view_summary": {
                "alias": "view_summary",
                "compiled_path": os.path.join(compiled_model_path, "view_summary.sql"),
                "build_path": None,
                "created_at": ANY,
                "columns": {
                    "first_name": {
                        "description": "The first name being summarized",
                        "name": "first_name",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "ct": {
                        "description": "The number of instances of the first name",
                        "name": "ct",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                },
                "config": get_rendered_model_config(),
                "database": project.database,
                "depends_on": {"macros": [], "nodes": ["model.test.ephemeral_summary"]},
                "deferred": False,
                "description": "A view of the summary of the ephemeral copy of the seed data",
                "docs": {"show": True},
                "fqn": ["test", "view_summary"],
                "name": "view_summary",
                "original_file_path": view_summary_path,
                "package_name": "test",
                "patch_path": "test://" + schema_yml_path,
                "path": "view_summary.sql",
                "raw_sql": LineIndifferent(view_summary_sql),
                "refs": [["ephemeral_summary"]],
                "relation_name": '"{0}"."{1}".view_summary'.format(model_database, my_schema_name),
                "resource_type": "model",
                "root_path": project.project_root,
                "schema": my_schema_name,
                "sources": [],
                "tags": [],
                "meta": {},
                "unique_id": "model.test.view_summary",
                "compiled": True,
                "compiled_sql": ANY,
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "checksum": checksum_file(view_summary_path),
                "unrendered_config": get_unrendered_model_config(materialized="view"),
            },
            "seed.test.seed": {
                "alias": "seed",
                "compiled_path": None,
                "build_path": None,
                "created_at": ANY,
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "The user ID number",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "first_name": {
                        "name": "first_name",
                        "description": "The user's first name",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "email": {
                        "name": "email",
                        "description": "The user's email",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "ip_address": {
                        "name": "ip_address",
                        "description": "The user's IP address",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                    "updated_at": {
                        "name": "updated_at",
                        "description": "The last time this user's email was updated",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    },
                },
                "config": get_rendered_seed_config(),
                "sources": [],
                "depends_on": {"macros": [], "nodes": []},
                "deferred": False,
                "description": "The test seed",
                "docs": {"show": True},
                "fqn": ["test", "seed"],
                "name": "seed",
                "original_file_path": seed_path,
                "package_name": "test",
                "patch_path": "test://" + os.path.join("seeds", "schema.yml"),
                "path": "seed.csv",
                "raw_sql": "",
                "refs": [],
                "relation_name": '"{0}"."{1}".seed'.format(model_database, my_schema_name),
                "resource_type": "seed",
                "root_path": project.project_root,
                "schema": my_schema_name,
                "database": project.database,
                "tags": [],
                "meta": {},
                "unique_id": "seed.test.seed",
                "compiled": True,
                "compiled_sql": "",
                "extra_ctes_injected": True,
                "extra_ctes": [],
                "checksum": checksum_file(seed_path),
                "unrendered_config": get_unrendered_seed_config(),
            },
            "snapshot.test.snapshot_seed": {
                "alias": "snapshot_seed",
                "compiled_path": None,
                "build_path": None,
                "created_at": ANY,
                "checksum": checksum_file(snapshot_path),
                "columns": {},
                "compiled": True,
                "compiled_sql": ANY,
                "config": get_rendered_snapshot_config(target_schema=alternate_schema),
                "database": model_database,
                "deferred": False,
                "depends_on": {"macros": [], "nodes": ["seed.test.seed"]},
                "description": "",
                "docs": {"show": True},
                "extra_ctes": [],
                "extra_ctes_injected": True,
                "fqn": ["test", "snapshot_seed", "snapshot_seed"],
                "meta": {},
                "name": "snapshot_seed",
                "original_file_path": snapshot_path,
                "package_name": "test",
                "patch_path": None,
                "path": "snapshot_seed.sql",
                "raw_sql": ANY,
                "refs": [["seed"]],
                "relation_name": '"{0}"."{1}".snapshot_seed'.format(
                    model_database, alternate_schema
                ),
                "resource_type": "snapshot",
                "root_path": project.project_root,
                "schema": alternate_schema,
                "sources": [],
                "tags": [],
                "unique_id": "snapshot.test.snapshot_seed",
                "unrendered_config": get_unrendered_snapshot_config(
                    target_schema=alternate_schema
                ),
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "columns": {
                    "id": {
                        "description": "An ID field",
                        "name": "id",
                        "data_type": None,
                        "meta": {},
                        "quote": None,
                        "tags": [],
                    }
                },
                "config": {
                    "enabled": True,
                },
                "quoting": {
                    "database": False,
                    "schema": None,
                    "identifier": True,
                    "column": None,
                },
                "created_at": ANY,
                "database": project.database,
                "description": "My table",
                "external": None,
                "freshness": {
                    "error_after": {"count": None, "period": None},
                    "warn_after": {"count": None, "period": None},
                    "filter": None,
                },
                "identifier": "seed",
                "loaded_at_field": None,
                "loader": "a_loader",
                "meta": {},
                "name": "my_table",
                "original_file_path": os.path.join("models", "schema.yml"),
                "package_name": "test",
                "path": os.path.join("models", "schema.yml"),
                "patch_path": None,
                "relation_name": '{0}."{1}"."seed"'.format(project.database, my_schema_name),
                "resource_type": "source",
                "root_path": project.project_root,
                "schema": my_schema_name,
                "source_description": "My source",
                "source_name": "my_source",
                "source_meta": {},
                "tags": [],
                "unique_id": "source.test.my_source.my_table",
                "fqn": ["test", "my_source", "my_table"],
                "unrendered_config": {},
            },
        },
        "exposures": {
            "exposure.test.notebook_exposure": {
                "created_at": ANY,
                "depends_on": {"macros": [], "nodes": ["model.test.view_summary"]},
                "description": "A description of the complex exposure",
                "fqn": ["test", "notebook_exposure"],
                "maturity": "medium",
                "meta": {"tool": "my_tool", "languages": ["python"]},
                "tags": ["my_department"],
                "name": "notebook_exposure",
                "original_file_path": os.path.join("models", "schema.yml"),
                "owner": {"email": "something@example.com", "name": "Some name"},
                "package_name": "test",
                "path": "schema.yml",
                "refs": [["view_summary"]],
                "resource_type": "exposure",
                "root_path": project.project_root,
                "sources": [],
                "type": "notebook",
                "unique_id": "exposure.test.notebook_exposure",
                "url": "http://example.com/notebook/1",
            },
        },
        "metrics": {},
        "selectors": {},
        "docs": {
            "dbt.__overview__": ANY,
            "test.column_info": {
                "block_contents": "An ID field",
                "name": "column_info",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.column_info",
            },
            "test.ephemeral_summary": {
                "block_contents": ("A summmary table of the ephemeral copy of the seed data"),
                "name": "ephemeral_summary",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.ephemeral_summary",
            },
            "test.source_info": {
                "block_contents": "My source",
                "name": "source_info",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.source_info",
            },
            "test.summary_count": {
                "block_contents": "The number of instances of the first name",
                "name": "summary_count",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.summary_count",
            },
            "test.summary_first_name": {
                "block_contents": "The first name being summarized",
                "name": "summary_first_name",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.summary_first_name",
            },
            "test.table_info": {
                "block_contents": "My table",
                "name": "table_info",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.table_info",
            },
            "test.view_summary": {
                "block_contents": (
                    "A view of the summary of the ephemeral copy of the " "seed data"
                ),
                "name": "view_summary",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.view_summary",
            },
            "test.macro_info": {
                "block_contents": "My custom test that I wrote that does nothing",
                "name": "macro_info",
                "original_file_path": os.path.join("macros", "macro.md"),
                "package_name": "test",
                "path": "macro.md",
                "root_path": project.project_root,
                "unique_id": "test.macro_info",
            },
            "test.notebook_info": {
                "block_contents": "A description of the complex exposure",
                "name": "notebook_info",
                "original_file_path": docs_path,
                "package_name": "test",
                "path": "docs.md",
                "root_path": project.project_root,
                "unique_id": "test.notebook_info",
            },
            "test.macro_arg_info": {
                "block_contents": "The model for my custom test",
                "name": "macro_arg_info",
                "original_file_path": os.path.join("macros", "macro.md"),
                "package_name": "test",
                "path": "macro.md",
                "root_path": project.project_root,
                "unique_id": "test.macro_arg_info",
            },
        },
        "child_map": {
            "model.test.ephemeral_copy": ["model.test.ephemeral_summary"],
            "exposure.test.notebook_exposure": [],
            "model.test.ephemeral_summary": ["model.test.view_summary"],
            "model.test.view_summary": ["exposure.test.notebook_exposure"],
            "seed.test.seed": ["snapshot.test.snapshot_seed"],
            "snapshot.test.snapshot_seed": [],
            "source.test.my_source.my_table": ["model.test.ephemeral_copy"],
        },
        "parent_map": {
            "model.test.ephemeral_copy": ["source.test.my_source.my_table"],
            "model.test.ephemeral_summary": ["model.test.ephemeral_copy"],
            "model.test.view_summary": ["model.test.ephemeral_summary"],
            "exposure.test.notebook_exposure": ["model.test.view_summary"],
            "seed.test.seed": [],
            "snapshot.test.snapshot_seed": ["seed.test.seed"],
            "source.test.my_source.my_table": [],
        },
        "disabled": {},
        "macros": {
            "macro.test.test_nothing": {
                "name": "test_nothing",
                "depends_on": {"macros": []},
                "created_at": ANY,
                "description": "My custom test that I wrote that does nothing",
                "docs": {"show": True},
                "macro_sql": AnyStringWith("test nothing"),
                "original_file_path": os.path.join("macros", "dummy_test.sql"),
                "path": os.path.join("macros", "dummy_test.sql"),
                "package_name": "test",
                "meta": {
                    "some_key": 100,
                },
                "patch_path": "test://" + os.path.join("macros", "schema.yml"),
                "resource_type": "macro",
                "unique_id": "macro.test.test_nothing",
                "tags": [],
                "root_path": project.project_root,
                "arguments": [
                    {
                        "name": "model",
                        "type": "Relation",
                        "description": "The model for my custom test",
                    },
                ],
            }
        },
    }
