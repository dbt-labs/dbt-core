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
      generate_latest_pointer: false
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
    latest_version: 2
    versions:
      - v: 1
      - v: 2
"""


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

        pointer_relation = relation_from_name(project.adapter, "disabled_pointer_model")
        with get_connection(project.adapter):
            relation = project.adapter.get_relation(
                pointer_relation.database,
                pointer_relation.schema,
                pointer_relation.identifier,
            )

        assert relation is None


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

        pointer_relation = relation_from_name(project.adapter, "versioned_model")
        with get_connection(project.adapter):
            relation = project.adapter.get_relation(
                pointer_relation.database,
                pointer_relation.schema,
                pointer_relation.identifier,
            )

        assert relation is None


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
