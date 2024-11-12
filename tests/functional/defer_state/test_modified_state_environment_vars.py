import os

import pytest

from dbt.tests.util import run_dbt
from tests.functional.defer_state.fixtures import (
    model_with_env_var_in_config_sql,
    model_with_no_in_config_sql,
    schema_model_with_env_var_in_config_yml,
)
from tests.functional.defer_state.test_modified_state import BaseModifiedState


class BaseTestStateSelectionEnvVarConfig(BaseModifiedState):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self):
        os.environ["DBT_TEST_STATE_MODIFIED"] = "table"
        yield
        del os.environ["DBT_TEST_STATE_MODIFIED"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": True,
            }
        }

    def test_change_env_var(self, project):
        # Generate ./state without changing environment variable value
        run_dbt(["run"])
        self.copy_state()

        # Assert no false positive
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0

        # Change environment variable and assert no false positive
        # Environment variables do not have an effect on state:modified
        os.environ["DBT_TEST_STATE_MODIFIED"] = "view"
        results = run_dbt(["list", "-s", "state:modified", "--state", "./state"])
        assert len(results) == 0


class TestModelNodeWithEnvVarConfigInSqlFile(BaseTestStateSelectionEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_env_var_in_config_sql,
        }


class TestModelNodeWithEnvVarConfigInSchemaYml(BaseTestStateSelectionEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
            "schema.yml": schema_model_with_env_var_in_config_yml,
        }


class TestModelNodeWithEnvVarConfigInProjectYml(BaseTestStateSelectionEnvVarConfig):
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
                    "+materialized": "{{ env_var('DBT_TEST_STATE_MODIFIED') }}",
                }
            }
        }


class TestModelNodeWithEnvVarConfigInProjectYmlAndSchemaYml(BaseTestStateSelectionEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_no_in_config_sql,
            "schema.yml": schema_model_with_env_var_in_config_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {
                "state_modified_compare_more_unrendered_values": True,
            },
            "models": {
                "test": {
                    "+materialized": "{{ env_var('DBT_TEST_STATE_MODIFIED') }}",
                }
            },
        }


class TestModelNodeWithEnvVarConfigInSqlAndSchemaYml(BaseTestStateSelectionEnvVarConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_with_env_var_in_config_sql,
            "schema.yml": schema_model_with_env_var_in_config_yml,
        }
