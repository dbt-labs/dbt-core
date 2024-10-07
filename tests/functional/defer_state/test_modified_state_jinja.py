import pytest

from dbt.tests.util import (
    get_artifact,
    get_project_config,
    run_dbt,
    update_config_file,
    write_file,
)
from tests.functional.defer_state.fixtures import (
    model_with_jinja_in_config_sql,
    model_with_no_in_config_sql,
    model_with_updated_jinja_in_config_sql,
    schema_model_with_jinja_in_config_yml,
    schema_model_with_updated_jinja_in_config_yml,
)
from tests.functional.defer_state.test_modified_state import BaseModifiedState


class BaseTestStateSelectionJinjaInConfig(BaseModifiedState):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": True,
            }
        }

    def update_jinja_expression_in_config(self, project):
        pass

    def test_change_jinja_if(self, project):
        run_dbt(["run"])
        self.copy_state()
        # Model is table when execute = True
        manifest_json = get_artifact(project.project_root, "target", "manifest.json")
        assert manifest_json["nodes"]["model.test.model"]["config"]["materialized"] == "view"

        # Assert no false positive (execute = False)
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Update unrendered config (change jinja expression)
        self.update_jinja_expression_in_config(project)
        # Assert no false negatives (jinja expression has changed)
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 1


class TestModelNodeWithJinjaConfigInSqlFile(BaseTestStateSelectionJinjaInConfig):
    def update_jinja_expression_in_config(self, project):
        write_file(
            model_with_updated_jinja_in_config_sql, project.project_root, "models", "model.sql"
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_jinja_in_config_sql,
        }


class TestModelNodeWithEnvVarConfigInSchemaYml(BaseTestStateSelectionJinjaInConfig):
    def update_jinja_expression_in_config(self, project):
        write_file(
            schema_model_with_updated_jinja_in_config_yml,
            project.project_root,
            "models",
            "schema.yml",
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
            "schema.yml": schema_model_with_jinja_in_config_yml,
        }


class TestModelNodeWithJinjaConfigInProjectYml(BaseTestStateSelectionJinjaInConfig):
    def update_jinja_expression_in_config(self, project):
        config = get_project_config(project)
        config["models"]["test"]["+materialized"] = "{{ ('view' if execute else 'table') }}"
        update_config_file(config, "dbt_project.yml")

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+materialized": "{{ ('table' if execute else 'view') }}",
                }
            }
        }


class TestModelNodeWithJinjaConfigInProjectYmlAndSchemaYml(BaseTestStateSelectionJinjaInConfig):
    def update_jinja_expression_in_config(self, project):
        write_file(
            schema_model_with_updated_jinja_in_config_yml,
            project.project_root,
            "models",
            "schema.yml",
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
            "schema.yml": schema_model_with_jinja_in_config_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": True,
            },
            "models": {
                "test": {
                    "+materialized": "{{ ('view' if execute else 'table') }}",
                }
            },
        }


class TestModelNodeWithJinjaConfigInSqlAndSchemaYml(BaseTestStateSelectionJinjaInConfig):
    def update_jinja_expression_in_config(self, project):
        write_file(
            model_with_updated_jinja_in_config_sql, project.project_root, "models", "model.sql"
        )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_jinja_in_config_sql,
            "schema.yml": schema_model_with_jinja_in_config_yml,
        }
