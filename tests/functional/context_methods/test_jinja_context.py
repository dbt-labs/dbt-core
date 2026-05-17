"""Reliability tests for the jinja context available during node execution.

These tests validate that expected context variables are present, have correct
types, and carry correct values across different resource types (models,
snapshots, singular tests, hooks). If anything is missing, wrong-typed, or
structurally broken, dbt raises a compiler error and the test fails — catching
regressions before they reach users who rely on these in custom macros.
"""

import pytest

from dbt.tests.util import run_dbt

# ---------------------------------------------------------------------------
# Shared helper macro — checks variables common to all resource types
# ---------------------------------------------------------------------------

_check_common_context_macro_sql = """
{% macro _check_common_context(expected_name, expected_resource_type, expected_id_prefix, check_subdir=true) %}

{# --- model dict: presence --- #}
{% if model is not mapping %}
    {{ exceptions.raise_compiler_error("`model` is not a mapping") }}
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

{# --- model dict: value correctness --- #}
{% elif model.name != expected_name %}
    {{ exceptions.raise_compiler_error("`model.name` expected '" ~ expected_name ~ "', got: " ~ model.name) }}
{% elif model.resource_type != expected_resource_type %}
    {{ exceptions.raise_compiler_error("`model.resource_type` expected '" ~ expected_resource_type ~ "', got: " ~ model.resource_type) }}
{% elif not model.unique_id.startswith(expected_id_prefix) %}
    {{ exceptions.raise_compiler_error("`model.unique_id` must start with '" ~ expected_id_prefix ~ "', got: " ~ model.unique_id) }}
{% elif model.path is not string or model.path == '' %}
    {{ exceptions.raise_compiler_error("`model.path` must be a non-empty string, got: " ~ model.path) }}
{% elif check_subdir and 'subdir' not in model.path %}
    {{ exceptions.raise_compiler_error("`model.path` must include 'subdir', got: " ~ model.path) }}
{% elif check_subdir and 'subdir' not in model.original_file_path %}
    {{ exceptions.raise_compiler_error("`model.original_file_path` must include 'subdir', got: " ~ model.original_file_path) }}

{# --- top-level scalars --- #}
{% elif database is not string or database == '' %}
    {{ exceptions.raise_compiler_error("`database` must be a non-empty string") }}
{% elif schema is not string or schema == '' %}
    {{ exceptions.raise_compiler_error("`schema` must be a non-empty string") }}
{% elif invocation_id is not string or invocation_id == '' %}
    {{ exceptions.raise_compiler_error("`invocation_id` must be a non-empty string") }}
{% elif dbt_version is not string or dbt_version == '' %}
    {{ exceptions.raise_compiler_error("`dbt_version` must be a non-empty string") }}

{# --- target --- #}
{% elif target is not mapping %}
    {{ exceptions.raise_compiler_error("`target` is not a mapping") }}
{% elif 'type' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'type'") }}
{% elif 'schema' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'schema'") }}
{% elif 'threads' not in target %}
    {{ exceptions.raise_compiler_error("`target` is missing key 'threads'") }}

{# --- execute --- #}
{% elif execute is undefined %}
    {{ exceptions.raise_compiler_error("`execute` is not defined") }}

{# --- core callables --- #}
{% elif ref is not callable %}
    {{ exceptions.raise_compiler_error("`ref` is not callable") }}
{% elif source is not callable %}
    {{ exceptions.raise_compiler_error("`source` is not callable") }}
{% elif var is not callable %}
    {{ exceptions.raise_compiler_error("`var` is not callable") }}
{% elif env_var is not callable %}
    {{ exceptions.raise_compiler_error("`env_var` is not callable") }}
{% elif config is not callable %}
    {{ exceptions.raise_compiler_error("`config` is not callable") }}

{# --- exceptions namespace --- #}
{% elif exceptions is not mapping %}
    {{ exceptions.raise_compiler_error("`exceptions` is not a mapping") }}
{% elif exceptions.raise_compiler_error is not callable %}
    {{ exceptions.raise_compiler_error("`exceptions.raise_compiler_error` is not callable") }}
{% elif exceptions.warn is not callable %}
    {{ exceptions.raise_compiler_error("`exceptions.warn` is not callable") }}

{# --- modules namespace --- #}
{% elif modules is not mapping %}
    {{ exceptions.raise_compiler_error("`modules` is not a mapping") }}
{% elif modules.datetime is undefined %}
    {{ exceptions.raise_compiler_error("`modules.datetime` is not available") }}
{% elif modules.re is undefined %}
    {{ exceptions.raise_compiler_error("`modules.re` is not available") }}

{# --- additional context objects --- #}
{% elif flags is undefined %}
    {{ exceptions.raise_compiler_error("`flags` is not defined") }}
{% elif graph is not mapping %}
    {{ exceptions.raise_compiler_error("`graph` is not a mapping") }}
{% elif api is not mapping %}
    {{ exceptions.raise_compiler_error("`api` is not a mapping") }}
{% elif adapter is undefined %}
    {{ exceptions.raise_compiler_error("`adapter` is not defined") }}
{% elif invocation_args_dict is not mapping %}
    {{ exceptions.raise_compiler_error("`invocation_args_dict` is not a mapping") }}
{% elif dbt_metadata_envs is not mapping %}
    {{ exceptions.raise_compiler_error("`dbt_metadata_envs` is not a mapping") }}
{% elif selected_resources is undefined %}
    {{ exceptions.raise_compiler_error("`selected_resources` is not defined") }}

{% endif %}
{% endmacro %}
"""

# ---------------------------------------------------------------------------
# Resource-specific macros
# ---------------------------------------------------------------------------

check_model_jinja_context_macro_sql = (
    _check_common_context_macro_sql
    + """
{% macro check_model_jinja_context() %}

{{ _check_common_context('context_model', 'model', 'model.') }}

{# --- model-specific callables --- #}
{% if is_incremental is not callable %}
    {{ exceptions.raise_compiler_error("`is_incremental` is not callable") }}
{% elif should_full_refresh is not callable %}
    {{ exceptions.raise_compiler_error("`should_full_refresh` is not callable") }}

{# --- this --- #}
{% elif this is undefined %}
    {{ exceptions.raise_compiler_error("`this` is not defined") }}

{# --- compiled_code / sql --- #}
{% elif compiled_code is defined and compiled_code and compiled_code is not string %}
    {{ exceptions.raise_compiler_error("`compiled_code` must be a string when set") }}
{% elif sql is defined and sql and sql is not string %}
    {{ exceptions.raise_compiler_error("`sql` must be a string when set") }}
{% endif %}

{% endmacro %}
"""
)

check_snapshot_jinja_context_macro_sql = (
    _check_common_context_macro_sql
    + """
{% macro check_snapshot_jinja_context() %}

{{ _check_common_context('context_snap', 'snapshot', 'snapshot.') }}

{# --- this --- #}
{% if this is undefined %}
    {{ exceptions.raise_compiler_error("`this` is not defined in snapshot context") }}
{% endif %}

{% endmacro %}
"""
)

check_test_jinja_context_macro_sql = (
    _check_common_context_macro_sql
    + """
{% macro check_test_jinja_context() %}

{{ _check_common_context('context_test', 'test', 'test.') }}

{# --- this --- #}
{% if this is undefined %}
    {{ exceptions.raise_compiler_error("`this` is not defined in test context") }}
{% endif %}

{% endmacro %}
"""
)

check_hook_jinja_context_macro_sql = """
{% macro check_hook_jinja_context() %}

{# Hooks use MacroContext which has fewer variables than ModelContext.
   model is the macro/operation node, not a user model, so we only
   check core context variables without model value assertions. #}

{# --- core callables --- #}
{% if ref is not callable %}
    {{ exceptions.raise_compiler_error("hook: `ref` is not callable") }}
{% elif source is not callable %}
    {{ exceptions.raise_compiler_error("hook: `source` is not callable") }}
{% elif var is not callable %}
    {{ exceptions.raise_compiler_error("hook: `var` is not callable") }}
{% elif env_var is not callable %}
    {{ exceptions.raise_compiler_error("hook: `env_var` is not callable") }}
{% elif config is not callable %}
    {{ exceptions.raise_compiler_error("hook: `config` is not callable") }}

{# --- target --- #}
{% elif target is not mapping %}
    {{ exceptions.raise_compiler_error("hook: `target` is not a mapping") }}
{% elif 'type' not in target %}
    {{ exceptions.raise_compiler_error("hook: `target` is missing key 'type'") }}
{% elif 'schema' not in target %}
    {{ exceptions.raise_compiler_error("hook: `target` is missing key 'schema'") }}

{# --- execute --- #}
{% elif execute is undefined %}
    {{ exceptions.raise_compiler_error("hook: `execute` is not defined") }}

{# --- scalars --- #}
{% elif database is not string or database == '' %}
    {{ exceptions.raise_compiler_error("hook: `database` must be a non-empty string") }}
{% elif schema is not string or schema == '' %}
    {{ exceptions.raise_compiler_error("hook: `schema` must be a non-empty string") }}
{% elif invocation_id is not string or invocation_id == '' %}
    {{ exceptions.raise_compiler_error("hook: `invocation_id` must be a non-empty string") }}
{% elif dbt_version is not string or dbt_version == '' %}
    {{ exceptions.raise_compiler_error("hook: `dbt_version` must be a non-empty string") }}

{# --- namespaces --- #}
{% elif exceptions is not mapping %}
    {{ exceptions.raise_compiler_error("hook: `exceptions` is not a mapping") }}
{% elif modules is not mapping %}
    {{ exceptions.raise_compiler_error("hook: `modules` is not a mapping") }}
{% elif flags is undefined %}
    {{ exceptions.raise_compiler_error("hook: `flags` is not defined") }}
{% elif graph is not mapping %}
    {{ exceptions.raise_compiler_error("hook: `graph` is not a mapping") }}
{% elif adapter is undefined %}
    {{ exceptions.raise_compiler_error("hook: `adapter` is not defined") }}

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

test_with_context_checks_sql = """
{{ check_test_jinja_context() }}
select 1 as failures where 1 = 0
"""

# The hook macro is called inline via on-run-start project config.
# A minimal model is needed so `dbt run` has something to execute.
hook_empty_model_sql = """
select 1 as id
"""

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestModelJinjaContext:
    """Validates jinja context during model execution (ModelContext)."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_model_jinja_context.sql": check_model_jinja_context_macro_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"subdir": {"context_model.sql": model_with_context_checks_sql}}

    def test_model_jinja_context(self, project):
        run_dbt(["run"])


class TestSnapshotJinjaContext:
    """Validates jinja context during snapshot execution (ModelContext)."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_snapshot_jinja_context.sql": check_snapshot_jinja_context_macro_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"subdir": {"context_snap.sql": snapshot_with_context_checks_sql}}

    def test_snapshot_jinja_context(self, project):
        run_dbt(["snapshot"])


class TestSingularTestJinjaContext:
    """Validates jinja context during singular test execution (ModelContext)."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_test_jinja_context.sql": check_test_jinja_context_macro_sql}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"subdir": {"context_test.sql": test_with_context_checks_sql}}

    def test_singular_test_jinja_context(self, project):
        # Singular tests need `dbt test` to run; the macro checks fire
        # during compilation and raise a compiler error on failure.
        run_dbt(["test"])


class TestHookJinjaContext:
    """Validates jinja context during on-run-start hook execution (MacroContext)."""

    @pytest.fixture(scope="class")
    def macros(self):
        return {"check_hook_jinja_context.sql": check_hook_jinja_context_macro_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"empty.sql": hook_empty_model_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"on-run-start": ["{{ check_hook_jinja_context() }}"]}

    def test_hook_jinja_context(self, project):
        run_dbt(["run"])
