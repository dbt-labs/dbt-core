import pytest

from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    get_connection,
    relation_from_name,
    run_dbt,
)

versioned_model_v1_sql = """
select 1 as id
"""

versioned_model_v2_sql = """
select 2 as id
"""

schema_yml = """
models:
  - name: versioned_model
    config:
      materialized: table
      latest_version_pointer:
        enabled: true
    latest_version: 2
    versions:
      - v: 1
      - v: 2
"""

disabled_pointer_schema_yml = """
models:
  - name: disabled_pointer_model
    config:
      materialized: table
      latest_version_pointer:
        enabled: false
    latest_version: 2
    versions:
      - v: 1
      - v: 2
"""

view_versioned_schema_yml = """
models:
  - name: view_versioned_model
    config:
      materialized: view
      latest_version_pointer:
        enabled: true
    latest_version: 2
    versions:
      - v: 1
      - v: 2
"""

custom_alias_schema_yml = """
models:
  - name: aliased_model
    config:
      materialized: table
      latest_version_pointer:
        enabled: true
        alias: my_custom_latest
    latest_version: 2
    versions:
      - v: 1
      - v: 2
"""

# No explicit latest_version_pointer config — project-level config controls it
project_disabled_schema_yml = """
models:
  - name: versioned_model
    config:
      materialized: table
    latest_version: 2
    versions:
      - v: 1
      - v: 2
"""


def assert_relation_does_not_exist(project, relation_name: str) -> None:
    """Assert that no relation with the given name exists in the test schema."""
    relation = relation_from_name(project.adapter, relation_name)
    with get_connection(project.adapter):
        result = project.adapter.get_relation(
            relation.database,
            relation.schema,
            relation.identifier,
        )
    assert result is None


class TestLatestVersionPointer:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "versioned_model_v1.sql": versioned_model_v1_sql,
            "versioned_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": schema_yml,
        }

    def test_run_creates_unsuffixed_latest_pointer(self, project):
        run_dbt(["run"])

        check_relation_types(
            project.adapter,
            {
                "versioned_model": "view",
                "versioned_model_v2": "table",
            },
        )
        check_relations_equal(project.adapter, ["versioned_model", "versioned_model_v2"])


class TestLatestVersionPointerIdempotent:
    """Running dbt run twice should succeed without errors."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "versioned_model_v1.sql": versioned_model_v1_sql,
            "versioned_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": schema_yml,
        }

    def test_run_twice_succeeds(self, project):
        run_dbt(["run"])
        run_dbt(["run"])

        check_relation_types(
            project.adapter,
            {
                "versioned_model": "view",
                "versioned_model_v2": "table",
            },
        )
        check_relations_equal(project.adapter, ["versioned_model", "versioned_model_v2"])


class TestLatestVersionPointerDisabled:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "disabled_pointer_model_v1.sql": versioned_model_v1_sql,
            "disabled_pointer_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": disabled_pointer_schema_yml,
        }

    def test_run_skips_pointer_when_disabled(self, project):
        run_dbt(["run"])
        assert_relation_does_not_exist(project, "disabled_pointer_model")


class TestLatestVersionPointerOnlyLatestVersion:
    """Running only a non-latest version should not create the pointer."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "versioned_model_v1.sql": versioned_model_v1_sql,
            "versioned_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": schema_yml,
        }

    def test_running_non_latest_skips_pointer(self, project):
        run_dbt(["run", "--select", "versioned_model.v1"])
        assert_relation_does_not_exist(project, "versioned_model")


class TestLatestVersionPointerViewMaterialization:
    """Pointer view works when the versioned model is itself a view."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_versioned_model_v1.sql": versioned_model_v1_sql,
            "view_versioned_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": view_versioned_schema_yml,
        }

    def test_pointer_created_for_view_model(self, project):
        run_dbt(["run"])

        check_relation_types(
            project.adapter,
            {
                "view_versioned_model": "view",
                "view_versioned_model_v2": "view",
            },
        )
        check_relations_equal(project.adapter, ["view_versioned_model", "view_versioned_model_v2"])


class TestLatestVersionPointerCustomAlias:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "aliased_model_v1.sql": versioned_model_v1_sql,
            "aliased_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": custom_alias_schema_yml,
        }

    def test_custom_alias_creates_correct_view(self, project):
        run_dbt(["run"])

        check_relation_types(
            project.adapter,
            {
                "my_custom_latest": "view",
                "aliased_model_v2": "table",
            },
        )
        check_relations_equal(project.adapter, ["my_custom_latest", "aliased_model_v2"])


class TestLatestVersionPointerProjectDisabled:
    """Project-level latest_version_pointer config disables the view even when flag is on."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"latest_version_pointer_enabled_by_default": True},
            "models": {"+latest_version_pointer": {"enabled": False}},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "versioned_model_v1.sql": versioned_model_v1_sql,
            "versioned_model_v2.sql": versioned_model_v2_sql,
            "schema.yml": project_disabled_schema_yml,
        }

    def test_project_level_disables_view(self, project):
        run_dbt(["run"])
        assert_relation_does_not_exist(project, "versioned_model")
