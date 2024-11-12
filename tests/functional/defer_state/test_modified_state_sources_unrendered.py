import os

import pytest

from dbt.tests.util import get_artifact, run_dbt, write_file
from tests.functional.defer_state.fixtures import (
    schema_source_with_env_var_as_database_property_yml,
    schema_source_with_env_var_as_schema_property_yml,
    schema_source_with_jinja_as_database_property_yml,
    schema_source_with_jinja_as_schema_property_yml,
    schema_source_with_updated_env_var_as_schema_property_yml,
    schema_source_with_updated_jinja_as_database_property_yml,
    schema_source_with_updated_jinja_as_schema_property_yml,
)
from tests.functional.defer_state.test_modified_state import BaseModifiedState
from tests.functional.defer_state.test_modified_state_environment_vars import (
    BaseTestStateSelectionEnvVarConfig,
)


class TestSourceNodeWithEnvVarConfigInDatabase(BaseTestStateSelectionEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_source_with_env_var_as_database_property_yml,
        }


class TestSourceNodeWithEnvVarConfigInSchema(BaseTestStateSelectionEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_source_with_env_var_as_schema_property_yml,
        }

    def test_change_env_var(self, project):
        # Generate ./state without changing environment variable value
        run_dbt(["run"])
        self.copy_state()

        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert manifest_json["sources"]["source.test.jaffle_shop.customers"]["schema"] == "table"
        assert (
            manifest_json["sources"]["source.test.jaffle_shop.customers"]["unrendered_schema"]
            == "{{ env_var('DBT_TEST_STATE_MODIFIED') }}"
        )

        # Assert no false positive
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Change environment variable and assert no false positive
        # Environment variables do not have an effect on state:modified
        os.environ["DBT_TEST_STATE_MODIFIED"] = "view"
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Confirm env change changed schema, but not unrendered_schema
        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert manifest_json["sources"]["source.test.jaffle_shop.customers"]["schema"] == "view"
        assert (
            manifest_json["sources"]["source.test.jaffle_shop.customers"]["unrendered_schema"]
            == "{{ env_var('DBT_TEST_STATE_MODIFIED') }}"
        )

        # Assert no false negative after actual change to schema
        write_file(
            schema_source_with_updated_env_var_as_schema_property_yml,
            project.project_root,
            "models",
            "schema.yml",
        )
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 1

        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert manifest_json["sources"]["source.test.jaffle_shop.customers"]["schema"] == "updated"
        assert (
            manifest_json["sources"]["source.test.jaffle_shop.customers"]["unrendered_schema"]
            == "updated"
        )


class TestSourceNodeWithJinjaInDatabase(BaseModifiedState):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": True,
            }
        }

    def update_jinja_expression_in_config(self, project):
        write_file(
            schema_source_with_updated_jinja_as_database_property_yml,
            project.project_root,
            "models",
            "schema.yml",
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_source_with_jinja_as_database_property_yml,
        }

    def test_change_jinja_if(self, project):
        run_dbt(["run"])
        self.copy_state()
        # source database is 'bar' when execute = False
        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert manifest_json["sources"]["source.test.jaffle_shop.customers"]["database"] == "bar"
        assert (
            manifest_json["sources"]["source.test.jaffle_shop.customers"]["unrendered_database"]
            == "{{ ('foo' if execute else 'bar') }}"
        )

        # Assert no false positive (execute = False)
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Update unrendered config (change jinja expression)
        self.update_jinja_expression_in_config(project)
        # Assert no false negatives (jinja expression has changed)
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 1


class TestSourceNodeWithJinjaInSchema(BaseModifiedState):
    def update_jinja_expression_in_config(self, project):
        write_file(
            schema_source_with_updated_jinja_as_schema_property_yml,
            project.project_root,
            "models",
            "schema.yml",
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": schema_source_with_jinja_as_schema_property_yml,
        }

    def test_change_jinja_if(self, project):
        run_dbt(["run"])
        self.copy_state()
        # source database is 'bar' when execute = False
        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert manifest_json["sources"]["source.test.jaffle_shop.customers"]["schema"] == "bar"
        assert (
            manifest_json["sources"]["source.test.jaffle_shop.customers"]["unrendered_schema"]
            == "{{ ('foo' if execute else 'bar') }}"
        )

        # Assert no false positive (execute = False)
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Update unrendered config (change jinja expression)
        self.update_jinja_expression_in_config(project)
        # Assert no false negatives (jinja expression has changed)
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 1
