"""Reliability tests for the jinja context available during node execution.

These tests validate that expected context variables are present, have correct
types, and carry correct values. If anything is missing, wrong-typed, or
structurally broken, dbt raises a compiler error and the test fails — catching
regressions before they reach users who rely on these in custom macros.

Pattern borrowed from tests/functional/microbatch/test_microbatch.py.
"""

import pytest

from dbt.tests.util import run_dbt

# ---------------------------------------------------------------------------
# Macros
# ---------------------------------------------------------------------------

check_model_jinja_context_macro_sql = """
{% macro check_model_jinja_context() %}

{# --- model dict --- #}
{% if model is not mapping %}
    {{ exceptions.raise_compiler_error("`model` is invalid: expected mapping type") }}

{# model must have expected keys #}
{% elif 'path' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'path'") }}
{% elif 'name' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'name'") }}
{% elif 'resource_type' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'resource_type'") }}
{% elif 'unique_id' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'unique_id'") }}
{% elif 'original_file_path' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'original_file_path'") }}
{% elif 'fqn' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'fqn'") }}
{% elif 'alias' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'alias'") }}
{% elif 'schema' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'schema'") }}
{% elif 'database' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'database'") }}

{# model field types and values #}
{% elif model.path is not string or model.path == '' %}
    {{ exceptions.raise_compiler_error("`model.path` is invalid: expected non-empty string, got: " ~ model.path) }}
{% elif 'subdir' not in model.path %}
    {{ exceptions.raise_compiler_error("`model.path` must include subdirectory 'subdir', got: " ~ model.path) }}
{% elif model.name != 'context_model' %}
    {{ exceptions.raise_compiler_error("`model.name` is invalid: expected 'context_model', got: " ~ model.name) }}
{% elif model.resource_type != 'model' %}
    {{ exceptions.raise_compiler_error("`model.resource_type` is invalid: expected 'model', got: " ~ model.resource_type) }}
{% elif not model.unique_id.startswith('model.') %}
    {{ exceptions.raise_compiler_error("`model.unique_id` must start with 'model.', got: " ~ model.unique_id) }}
{% elif 'subdir' not in model.original_file_path %}
    {{ exceptions.raise_compiler_error("`model.original_file_path` must include 'subdir', got: " ~ model.original_file_path) }}

{# --- top-level scalars --- #}
{% elif database is not string or database == '' %}
    {{ exceptions.raise_compiler_error("`database` is invalid: expected non-empty string") }}
{% elif schema is not string or schema == '' %}
    {{ exceptions.raise_compiler_error("`schema` is invalid: expected non-empty string") }}
{% elif invocation_id is not string or invocation_id == '' %}
    {{ exceptions.raise_compiler_error("`invocation_id` is invalid: expected non-empty string") }}
{% elif dbt_version is not string or dbt_version == '' %}
    {{ exceptions.raise_compiler_error("`dbt_version` is invalid: expected non-empty string") }}

{# --- target --- #}
{% elif target is not mapping %}
    {{ exceptions.raise_compiler_error("`target` is invalid: expected mapping type") }}
{% elif 'type' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'type'") }}
{% elif 'schema' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'schema'") }}
{% elif 'threads' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'threads'") }}

{# --- execute --- #}
{% elif execute is undefined %}
    {{ exceptions.raise_compiler_error("`execute` is not defined in model context") }}

{# --- callables --- #}
{% elif ref is not callable %}
    {{ exceptions.raise_compiler_error("`ref` is invalid: expected callable type") }}
{% elif source is not callable %}
    {{ exceptions.raise_compiler_error("`source` is invalid: expected callable type") }}
{% elif var is not callable %}
    {{ exceptions.raise_compiler_error("`var` is invalid: expected callable type") }}
{% elif env_var is not callable %}
    {{ exceptions.raise_compiler_error("`env_var` is invalid: expected callable type") }}
{% elif config is not callable %}
    {{ exceptions.raise_compiler_error("`config` is invalid: expected callable type") }}
{% elif is_incremental is not callable %}
    {{ exceptions.raise_compiler_error("`is_incremental` is invalid: expected callable type") }}
{% elif should_full_refresh is not callable %}
    {{ exceptions.raise_compiler_error("`should_full_refresh` is invalid: expected callable type") }}

{# --- exceptions namespace --- #}
{% elif exceptions is not mapping %}
    {{ exceptions.raise_compiler_error("`exceptions` is invalid: expected mapping type") }}
{% elif exceptions.raise_compiler_error is not callable %}
    {{ exceptions.raise_compiler_error("`exceptions.raise_compiler_error` is invalid: expected callable type") }}
{% elif exceptions.warn is not callable %}
    {{ exceptions.raise_compiler_error("`exceptions.warn` is invalid: expected callable type") }}

{# --- modules --- #}
{% elif modules is not mapping %}
    {{ exceptions.raise_compiler_error("`modules` is invalid: expected mapping type") }}
{% elif modules.datetime is undefined %}
    {{ exceptions.raise_compiler_error("`modules.datetime` is not available") }}
{% elif modules.re is undefined %}
    {{ exceptions.raise_compiler_error("`modules.re` is not available") }}

{# --- compiled_code / sql (may be None before execution) --- #}
{% elif compiled_code is defined and compiled_code and compiled_code is not string %}
    {{ exceptions.raise_compiler_error("`compiled_code` is invalid: expected string type") }}
{% elif sql is defined and sql and sql is not string %}
    {{ exceptions.raise_compiler_error("`sql` is invalid: expected string type") }}

{% endif %}

{% endmacro %}
"""

check_snapshot_jinja_context_macro_sql = """
{% macro check_snapshot_jinja_context() %}

{# --- model dict --- #}
{% if model is not mapping %}
    {{ exceptions.raise_compiler_error("`model` is invalid: expected mapping type") }}

{# model must have expected keys #}
{% elif 'path' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'path'") }}
{% elif 'name' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'name'") }}
{% elif 'resource_type' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'resource_type'") }}
{% elif 'unique_id' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'unique_id'") }}
{% elif 'original_file_path' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'original_file_path'") }}
{% elif 'fqn' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'fqn'") }}
{% elif 'alias' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'alias'") }}
{% elif 'schema' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'schema'") }}
{% elif 'database' not in model %}
    {{ exceptions.raise_compiler_error("`model` is missing key 'database'") }}

{# model field types and values #}
{% elif model.path is not string or model.path == '' %}
    {{ exceptions.raise_compiler_error("`model.path` is invalid: expected non-empty string, got: " ~ model.path) }}
{% elif 'subdir' not in model.path %}
    {{ exceptions.raise_compiler_error("`model.path` must include subdirectory 'subdir', got: " ~ model.path) }}
{% elif model.name != 'context_snap' %}
    {{ exceptions.raise_compiler_error("`model.name` is invalid: expected 'context_snap', got: " ~ model.name) }}
{% elif model.resource_type != 'snapshot' %}
    {{ exceptions.raise_compiler_error("`model.resource_type` is invalid: expected 'snapshot', got: " ~ model.resource_type) }}
{% elif not model.unique_id.startswith('snapshot.') %}
    {{ exceptions.raise_compiler_error("`model.unique_id` must start with 'snapshot.', got: " ~ model.unique_id) }}
{% elif 'subdir' not in model.original_file_path %}
    {{ exceptions.raise_compiler_error("`model.original_file_path` must include 'subdir', got: " ~ model.original_file_path) }}

{# --- top-level scalars --- #}
{% elif database is not string or database == '' %}
    {{ exceptions.raise_compiler_error("`database` is invalid: expected non-empty string") }}
{% elif schema is not string or schema == '' %}
    {{ exceptions.raise_compiler_error("`schema` is invalid: expected non-empty string") }}
{% elif invocation_id is not string or invocation_id == '' %}
    {{ exceptions.raise_compiler_error("`invocation_id` is invalid: expected non-empty string") }}
{% elif dbt_version is not string or dbt_version == '' %}
    {{ exceptions.raise_compiler_error("`dbt_version` is invalid: expected non-empty string") }}

{# --- target --- #}
{% elif target is not mapping %}
    {{ exceptions.raise_compiler_error("`target` is invalid: expected mapping type") }}
{% elif 'type' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'type'") }}
{% elif 'schema' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'schema'") }}
{% elif 'threads' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'threads'") }}

{# --- execute --- #}
{% elif execute is undefined %}
    {{ exceptions.raise_compiler_error("`execute` is not defined in snapshot context") }}

{# --- callables --- #}
{% elif ref is not callable %}
    {{ exceptions.raise_compiler_error("`ref` is invalid: expected callable type") }}
{% elif source is not callable %}
    {{ exceptions.raise_compiler_error("`source` is invalid: expected callable type") }}
{% elif var is not callable %}
    {{ exceptions.raise_compiler_error("`var` is invalid: expected callable type") }}
{% elif env_var is not callable %}
    {{ exceptions.raise_compiler_error("`env_var` is invalid: expected callable type") }}
{% elif config is not callable %}
    {{ exceptions.raise_compiler_error("`config` is invalid: expected callable type") }}

{# --- exceptions namespace --- #}
{% elif exceptions is not mapping %}
    {{ exceptions.raise_compiler_error("`exceptions` is invalid: expected mapping type") }}
{% elif exceptions.raise_compiler_error is not callable %}
    {{ exceptions.raise_compiler_error("`exceptions.raise_compiler_error` is invalid: expected callable type") }}
{% elif exceptions.warn is not callable %}
    {{ exceptions.raise_compiler_error("`exceptions.warn` is invalid: expected callable type") }}

{# --- modules --- #}
{% elif modules is not mapping %}
    {{ exceptions.raise_compiler_error("`modules` is invalid: expected mapping type") }}
{% elif modules.datetime is undefined %}
    {{ exceptions.raise_compiler_error("`modules.datetime` is not available") }}
{% elif modules.re is undefined %}
    {{ exceptions.raise_compiler_error("`modules.re` is not available") }}

{# --- compiled_code / sql (may be None before execution) --- #}
{% elif compiled_code is defined and compiled_code and compiled_code is not string %}
    {{ exceptions.raise_compiler_error("`compiled_code` is invalid: expected string type") }}
{% elif sql is defined and sql and sql is not string %}
    {{ exceptions.raise_compiler_error("`sql` is invalid: expected string type") }}

{% endif %}

{% endmacro %}
"""

# ---------------------------------------------------------------------------
# SQL fixtures
# ---------------------------------------------------------------------------

model_with_context_checks_sql = """
{{ check_model_jinja_context() }}
select 1 as id
"""

snapshot_with_context_checks_sql = """
{% snapshot context_snap %}
    {{
        config(
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols='all',
        )
    }}
    {{ check_snapshot_jinja_context() }}
    select 1 as id, 'alice' as name
{% endsnapshot %}
"""

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestModelJinjaContext:
    """Validates that all expected jinja context variables are present,
    correctly typed, and carry correct values during model execution.

    If any variable is missing, wrong-typed, or structurally broken, dbt
    raises a compiler error and the test fails.
    """

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_model_jinja_context.sql": check_model_jinja_context_macro_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"subdir": {"context_model.sql": model_with_context_checks_sql}}

    def test_model_jinja_context(self, project):
        run_dbt(["run"])


class TestSnapshotJinjaContext:
    """Validates that all expected jinja context variables are present,
    correctly typed, and carry correct values during snapshot execution.

    If any variable is missing, wrong-typed, or structurally broken, dbt
    raises a compiler error and the test fails.
    """

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_snapshot_jinja_context.sql": check_snapshot_jinja_context_macro_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"subdir": {"context_snap.sql": snapshot_with_context_checks_sql}}

    def test_snapshot_jinja_context(self, project):
        run_dbt(["snapshot"])
